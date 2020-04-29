package org.prebid.server.handler;

import io.netty.util.AsciiString;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.VertxTest;
import org.prebid.server.analytics.AnalyticsReporter;
import org.prebid.server.analytics.model.SetuidEvent;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.bidder.Usersyncer;
import org.prebid.server.cookie.UidsCookie;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.cookie.model.UidWithExpiry;
import org.prebid.server.cookie.proto.Uids;
import org.prebid.server.exception.InvalidRequestException;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.TimeoutFactory;
import org.prebid.server.metric.Metrics;
import org.prebid.server.privacy.gdpr.TcfDefinerService;
import org.prebid.server.privacy.gdpr.model.PrivacyEnforcementAction;
import org.prebid.server.privacy.gdpr.model.TcfResponse;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.rubicon.audit.proto.UidAudit;
import org.prebid.server.settings.ApplicationSettings;
import org.prebid.server.settings.model.Account;
import org.prebid.server.settings.model.AccountGdprConfig;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class SetuidHandlerTest extends VertxTest {

    private static final String RUBICON = "rubicon";
    private static final String FACEBOOK = "audienceNetwork";
    private static final String ADNXS = "adnxs";

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private UidsCookieService uidsCookieService;
    @Mock
    private UidsAuditCookieService uidsAuditCookieService;
    @Mock
    private ApplicationSettings applicationSettings;
    @Mock
    private BidderCatalog bidderCatalog;
    @Mock
    private TcfDefinerService tcfDefinerService;
    @Mock
    private AnalyticsReporter analyticsReporter;
    @Mock
    private Metrics metrics;

    private SetuidHandler setuidHandler;
    @Mock
    private RoutingContext routingContext;
    @Mock
    private HttpServerRequest httpRequest;
    @Mock
    private HttpServerResponse httpResponse;
    @Mock
    private MultiMap responseHeaders;

    @Before
    public void setUp() {
        final Map<Integer, PrivacyEnforcementAction> vendorIdToGdpr = singletonMap(null,
                PrivacyEnforcementAction.allowAll());
        given(tcfDefinerService.resultForVendorIds(anySet(), any(), any(), any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(TcfResponse.of(true, vendorIdToGdpr, null)));

        given(routingContext.request()).willReturn(httpRequest);
        given(routingContext.response()).willReturn(httpResponse);

        given(httpResponse.headers()).willReturn(new CaseInsensitiveHeaders());

        given(uidsCookieService.toCookie(any())).willReturn(Cookie.cookie("test", "test"));

        given(applicationSettings.getAccountById(any(), any())).willReturn(Future.failedFuture("not found"));

        given(bidderCatalog.names()).willReturn(new HashSet<>(asList("rubicon", "appnexus", "audienceNetwork")));
        given(bidderCatalog.isActive(any())).willReturn(true);
        given(bidderCatalog.usersyncerByName(any())).willReturn(
                new Usersyncer(RUBICON, null, null, null, false));
        given(bidderCatalog.usersyncerByName("appnexus")).willReturn(
                new Usersyncer(ADNXS, null, null, null, false));

        given(httpRequest.getParam("account")).willReturn("account");
        given(httpResponse.headers()).willReturn(responseHeaders);

        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(2000, uidsCookieService, applicationSettings, bidderCatalog,
                tcfDefinerService, null, false, analyticsReporter, metrics, timeoutFactory, true,
                uidsAuditCookieService);
    }

    @Test
    public void shouldRespondWithEmptyBodyAndNoContentStatusIfCookieIsDisabled() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(2000, uidsCookieService, applicationSettings, bidderCatalog,
                tcfDefinerService, null, false, analyticsReporter, metrics, timeoutFactory, false,
                uidsAuditCookieService);
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(204));
        verify(httpResponse).end();
    }

    @Test
    public void shouldRespondWithErrorIfOptedOut() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).optout(true).build(), jacksonMapper));

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(401));
        verify(httpResponse).end();
    }

    @Test
    public void shouldRespondWithErrorIfBidderParamIsMissing() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end(eq("\"bidder\" query param is required"));
    }

    @Test
    public void shouldRespondWithErrorIfUserInGdprScopeAndAccountParamIsMissing() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));
        given(httpRequest.getParam("account")).willReturn(null);
        given(httpRequest.getParam("bidder")).willReturn(RUBICON);

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end(eq("\"account\" query param is required"));
    }

    @Test
    public void shouldRespondWithErrorIfBidderParamIsInvalid() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));
        given(httpRequest.getParam(any())).willReturn("invalid_or_disabled");
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end(eq("\"bidder\" query param is invalid"));
    }

    @Test
    public void shouldRespondWithoutCookieIfGdprProcessingPreventsCookieSetting() {
        // given
        final PrivacyEnforcementAction privacyEnforcementAction = PrivacyEnforcementAction.restrictAll();
        given(tcfDefinerService.resultForVendorIds(anySet(), any(), any(), any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(
                        TcfResponse.of(true, singletonMap(null, privacyEnforcementAction), null)));

        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).setStatusCode(eq(200));
        verify(httpResponse).end(eq("The gdpr_consent param prevents cookies from being saved"));
    }

    @Test
    public void shouldRespondWithBadRequestStatusIfGdprProcessingFailsWithInvalidRequestException() {
        // given
        given(tcfDefinerService.resultForVendorIds(anySet(), any(), any(), any(), any(), any(), any()))
                .willReturn(Future.failedFuture(new InvalidRequestException("gdpr exception")));

        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end(eq("GDPR processing failed with error: gdpr exception"));
    }

    @Test
    public void shouldRespondWithInternalServerErrorStatusIfGdprProcessingFailsWithUnexpectedException() {
        // given
        given(tcfDefinerService.resultForVendorIds(anySet(), any(), any(), any(), any(), any(), any()))
                .willReturn(Future.failedFuture("unexpected error"));

        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse, never()).sendFile(any());
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).setStatusCode(eq(500));
        verify(httpResponse).end(eq("Unexpected GDPR processing error"));
    }

    @Test
    public void shouldPassIpAddressToTcfDefinerServiceIfGeoLocationEnabled() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(2000, uidsCookieService, applicationSettings, bidderCatalog,
                tcfDefinerService, null, true, analyticsReporter, metrics, timeoutFactory, true,
                uidsAuditCookieService);

        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        final MultiMap headers = mock(MultiMap.class);
        given(httpRequest.headers()).willReturn(headers);
        given(headers.get("X-Forwarded-For")).willReturn("192.168.144.1");

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(tcfDefinerService).resultForVendorIds(anySet(), any(), any(), eq("192.168.144.1"), any(), any(), any());
    }

    @Test
    public void shouldPassAccountToTcfDefinerServiceWhenAccountIsFound() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("account")).willReturn("accId");

        final AccountGdprConfig accountGdprConfig = AccountGdprConfig.builder().enabled(true).build();
        final Account account = Account.builder().gdpr(accountGdprConfig).build();
        final Future<Account> accountFuture = Future.succeededFuture(account);
        given(applicationSettings.getAccountById(any(), any())).willReturn(accountFuture);

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(applicationSettings).getAccountById(eq("accId"), any());
        verify(tcfDefinerService).resultForVendorIds(anySet(), any(), any(), any(), eq(accountGdprConfig), any(),
                any());
    }

    @Test
    public void shouldPassAccountToTcfDefinerServiceWhenAccountIsNotFound() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("account")).willReturn("accId");

        given(applicationSettings.getAccountById(any(), any())).willReturn(Future.failedFuture("bad req"));

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(applicationSettings).getAccountById(eq("accId"), any());
        verify(tcfDefinerService).resultForVendorIds(anySet(), any(), any(), any(), isNull(), any(), any());
    }

    @Test
    public void shouldRemoveUidFromCookieIfMissingInRequest() throws IOException {
        // given
        final Map<String, UidWithExpiry> uids = new HashMap<>();
        uids.put(RUBICON, UidWithExpiry.live("J5VLCWQP-26-CWFT"));
        uids.put(ADNXS, UidWithExpiry.live("12345"));
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(uids).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("format")).willReturn("img");

        // this uids cookie stands for {"tempUIDs":{"adnxs":{"uid":"12345"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJhZG54cyI6eyJ1aWQiOiIxMjM0NSJ9fX0="));

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).sendFile(any());

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(ADNXS).getUid()).isEqualTo("12345");
    }

    @Test
    public void shouldIgnoreFacebookSentinel() throws IOException {
        // given
        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(FACEBOOK, UidWithExpiry.live("facebookUid"))).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(FACEBOOK);
        given(httpRequest.getParam("uid")).willReturn("0");

        // this uids cookie value stands for {"tempUIDs":{"audienceNetwork":{"uid":"facebookUid"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJhdWRpZW5jZU5ldHdvcmsiOnsidWlkIjoiZmFjZWJvb2tVaWQifX19"));

        given(uidsAuditCookieService.getUidsAudit(any(RoutingContext.class))).willReturn(UidAudit.builder().build());
        given(uidsAuditCookieService.createUidsAuditCookie(any(), any(), any(), any(), any(), any()))
                .willReturn(Cookie.cookie("uids-audit", "value").setDomain("rubicon"));

        given(bidderCatalog.usersyncerByName(any())).willReturn(
                new Usersyncer(FACEBOOK, null, null, null, false));

        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(2000, uidsCookieService, applicationSettings, bidderCatalog,
                tcfDefinerService, null, false, analyticsReporter, metrics, timeoutFactory, true,
                uidsAuditCookieService);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).end();
        verify(httpResponse, never()).sendFile(any());

        final String uidsCookie = captureAllCookies().get(0);
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(FACEBOOK).getUid()).isEqualTo("facebookUid");
    }

    @Test
    public void shouldRespondWithCookieFromRequestParam() throws IOException {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        // {"tempUIDs":{"rubicon":{"uid":"J5VLCWQP-26-CWFT"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJydWJpY29uIjp7InVpZCI6Iko1VkxDV1FQLTI2LUNXRlQifX19"));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("format")).willReturn("img");
        given(httpRequest.getParam("uid")).willReturn("J5VLCWQP-26-CWFT");

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).sendFile(any());

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(RUBICON).getUid()).isEqualTo("J5VLCWQP-26-CWFT");
    }

    @Test
    public void shouldUpdateUidInCookieWithRequestValue() throws IOException {
        // given
        final Map<String, UidWithExpiry> uids = new HashMap<>();
        uids.put(RUBICON, UidWithExpiry.live("J5VLCWQP-26-CWFT"));
        uids.put(ADNXS, UidWithExpiry.live("12345"));
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(uids).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("uid")).willReturn("updatedUid");

        // {"tempUIDs":{"adnxs":{"uid":"12345"}, "rubicon":{"uid":"updatedUid"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJhZG54cyI6eyJ1aWQiOiIxMjM0NSJ9LCAicnViaWNvbiI6eyJ1aWQiOiJ1cGRhdGVkVW"
                        + "lkIn19fQ=="));

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).end();
        verify(routingContext, never()).addCookie(any(Cookie.class));

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(2);
        assertThat(decodedUids.getUids().get(RUBICON).getUid()).isEqualTo("updatedUid");
        assertThat(decodedUids.getUids().get(ADNXS).getUid()).isEqualTo("12345");
    }

    @Test
    public void shouldRespondWithCookieIfUserIsNotInGdprScope() throws IOException {
        // given
        given(tcfDefinerService.resultForVendorIds(anySet(), any(), any(), any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(TcfResponse.of(false, emptyMap(), null)));

        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        // {"tempUIDs":{"rubicon":{"uid":"J5VLCWQP-26-CWFT"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJydWJpY29uIjp7InVpZCI6Iko1VkxDV1FQLTI2LUNXRlQifX19"));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("uid")).willReturn("J5VLCWQP-26-CWFT");

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).end();

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(RUBICON).getUid()).isEqualTo("J5VLCWQP-26-CWFT");
    }

    @Test
    public void shouldSendBadRequestIfUidsAuditCookieCannotBeCreated() {
        // given
        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(RUBICON, UidWithExpiry.live("J5VLCWQP-26-CWFT"))).build(),
                jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        given(uidsAuditCookieService.createUidsAuditCookie(any(), any(), any(), any(), any(), any()))
                .willThrow(new PreBidException("error"));

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end(
                "Error occurred on uids-audit cookie creation, uid cookie will not be set without it: error");
    }

    @Test
    public void shouldSendUidsAuditCookieWithUidsCookie() {
        // given
        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(RUBICON, UidWithExpiry.live("J5VLCWQP-26-CWFT"))).build(),
                jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);

        given(uidsAuditCookieService.createUidsAuditCookie(any(), any(), any(), any(), any(), any()))
                .willReturn(Cookie.cookie("uids-audit", "value").setDomain("rubicon"));

        // {"tempUIDs":{"adnxs":{"uid":"12345"}, "rubicon":{"uid":"updatedUid"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJhZG54cyI6eyJ1aWQiOiIxMjM0NSJ9LCAicnViaWNvbiI6eyJ1aWQiOiJ1cGRhdGVkVW"
                        + "lkIn19fQ=="));

        // when
        setuidHandler.handle(routingContext);

        // then
        final List<String> cookies = captureAllCookies();
        assertThat(cookies.get(0)).startsWith("uids=");
        assertThat(cookies.get(1)).startsWith("uids-audit=");
    }

    @Test
    public void shouldNotSendUidsAuditCookieButUidsCookieIfUserIsNotInGdprScope() throws IOException {
        // given
        given(tcfDefinerService.resultForVendorIds(anySet(), any(), any(), any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(TcfResponse.of(false, emptyMap(), null)));

        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(RUBICON, UidWithExpiry.live("J5VLCWQP-26-CWFT"))).build(),
                jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);

        // {"tempUIDs":{"rubicon":{"uid":"J5VLCWQP-26-CWFT"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJydWJpY29uIjp7InVpZCI6Iko1VkxDV1FQLTI2LUNXRlQifX19"));

        // when
        setuidHandler.handle(routingContext);

        // then
        verifyZeroInteractions(uidsAuditCookieService);

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(RUBICON).getUid()).isEqualTo("J5VLCWQP-26-CWFT");
    }

    @Test
    public void shouldNotSendUidsAuditCookieButUidsCookieIfUserIsNotInGdprScopeForNonRubiconBidder()
            throws IOException {
        // given
        given(tcfDefinerService.resultForVendorIds(anySet(), any(), any(), any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(TcfResponse.of(false, emptyMap(), null)));

        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(ADNXS, UidWithExpiry.live("12345"))).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(ADNXS);

        // this uids cookie stands for {"tempUIDs":{"adnxs":{"uid":"12345"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJhZG54cyI6eyJ1aWQiOiIxMjM0NSJ9fX0="));

        // when
        setuidHandler.handle(routingContext);

        // then
        verifyZeroInteractions(uidsAuditCookieService);

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(ADNXS).getUid()).isEqualTo("12345");
    }

    @Test
    public void shouldSendBadRequestIfUidsAuditCookieCannotBeRetrievedForNonRubiconBidder() {
        // given
        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(ADNXS, UidWithExpiry.live("12345"))).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(ADNXS);
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        given(uidsAuditCookieService.getUidsAudit(any(RoutingContext.class))).willThrow(new PreBidException("error"));

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end("Error retrieving of uids-audit cookie: error");
    }

    @Test
    public void shouldSendBadRequestIfUidsAuditCookieIsMissingForNonRubiconBidder() {
        // given
        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(ADNXS, UidWithExpiry.live("12345"))).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(ADNXS);
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        given(uidsAuditCookieService.getUidsAudit(any(RoutingContext.class))).willReturn(null);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end("\"uids-audit\" cookie is missing, sync Rubicon bidder first");
    }

    @Test
    public void shouldNotRequireAccountFromApplicationSettingForEmptyAccount() {
        // given
        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(ADNXS, UidWithExpiry.live("12345"))).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(ADNXS);
        given(httpRequest.getParam("account")).willReturn("");

        // when
        setuidHandler.handle(routingContext);

        // then
        verifyZeroInteractions(applicationSettings);
    }

    @Test
    public void shouldUpdateOptOutsMetricIfOptedOut() {
        // given
        // this uids cookie value stands for {"optout": true}
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).optout(true).build(), jacksonMapper));

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(metrics).updateUserSyncOptoutMetric();
    }

    @Test
    public void shouldUpdateBadRequestsMetricIfBidderParamIsMissing() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(metrics).updateUserSyncBadRequestMetric();
    }

    @Test
    public void shouldNotSendResponseIfClientClosedConnection() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("uid")).willReturn("uid");

        given(routingContext.response().closed()).willReturn(true);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse, never()).end();
    }

    @Test
    public void shouldUpdateSetsMetric() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("uid")).willReturn("updatedUid");

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(metrics).updateUserSyncSetsMetric(eq(RUBICON));
    }

    @Test
    public void shouldPassUnauthorizedEventToAnalyticsReporterIfOptedOut() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).optout(true).build(), jacksonMapper));

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.builder().status(401).build());
    }

    @Test
    public void shouldPassBadRequestEventToAnalyticsReporterIfBidderParamIsMissing() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.builder().status(400).build());
    }

    @Test
    public void shouldPassUnsuccessfulEventToAnalyticsReporterIfUidMissingInRequest() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);

        // when
        setuidHandler.handle(routingContext);

        // then
        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.builder()
                .status(200)
                .bidder(RUBICON)
                .success(false)
                .build());
    }

    @Test
    public void shouldPassUnsuccessfulEventToAnalyticsReporterIfFacebookSentinel() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(FACEBOOK);
        given(httpRequest.getParam("uid")).willReturn("0");

        given(uidsAuditCookieService.getUidsAudit(any(RoutingContext.class))).willReturn(UidAudit.builder().build());
        given(uidsAuditCookieService.createUidsAuditCookie(any(), any(), any(), any(), any(), any()))
                .willReturn(Cookie.cookie("uids-audit", "value").setDomain("rubicon"));
        given(bidderCatalog.usersyncerByName(any())).willReturn(
                new Usersyncer(FACEBOOK, null, null, null, false));

        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(2000, uidsCookieService, applicationSettings,
                bidderCatalog, tcfDefinerService, null, false, analyticsReporter, metrics, timeoutFactory, true,
                uidsAuditCookieService);

        // when
        setuidHandler.handle(routingContext);

        // then
        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.builder()
                .status(200)
                .bidder(FACEBOOK)
                .uid("0")
                .success(false)
                .build());
    }

    @Test
    public void shouldPassSuccessfulEventToAnalyticsReporter() {
        // given
        given(uidsCookieService.parseFromRequest(any())).willReturn(new UidsCookie(
                Uids.builder().uids(singletonMap(RUBICON, UidWithExpiry.live("J5VLCWQP-26-CWFT"))).build(),
                jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("uid")).willReturn("updatedUid");

        // when
        setuidHandler.handle(routingContext);

        // then
        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.builder()
                .status(200)
                .bidder(RUBICON)
                .uid("updatedUid")
                .success(true)
                .build());
    }

    private String captureCookie() {
        final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(responseHeaders).add(eq(new AsciiString("Set-Cookie")), captor.capture());
        return captor.getValue();
    }

    private List<String> captureAllCookies() {
        final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(responseHeaders, times(2)).add(eq(new AsciiString("Set-Cookie")), captor.capture());
        return captor.getAllValues();
    }

    private static Uids decodeUids(String value) throws IOException {
        final String uids = value.substring(5).split(";")[0];
        return mapper.readValue(Base64.getUrlDecoder().decode(uids), Uids.class);
    }

    private SetuidEvent captureSetuidEvent() {
        final ArgumentCaptor<SetuidEvent> setuidEventCaptor = ArgumentCaptor.forClass(SetuidEvent.class);
        verify(analyticsReporter).processEvent(setuidEventCaptor.capture());
        return setuidEventCaptor.getValue();
    }
}
