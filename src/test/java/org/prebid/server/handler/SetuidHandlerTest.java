package org.prebid.server.handler;

import io.netty.util.AsciiString;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.impl.SocketAddressImpl;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.VertxTest;
import org.prebid.server.analytics.AnalyticsReporterDelegator;
import org.prebid.server.analytics.model.SetuidEvent;
import org.prebid.server.auction.PrivacyEnforcementService;
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
import org.prebid.server.privacy.gdpr.model.TcfContext;
import org.prebid.server.privacy.gdpr.model.TcfResponse;
import org.prebid.server.privacy.model.PrivacyContext;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.rubicon.audit.proto.UidAudit;
import org.prebid.server.settings.ApplicationSettings;
import org.prebid.server.settings.model.Account;
import org.prebid.server.settings.model.AccountGdprConfig;
import org.prebid.server.settings.model.EnabledForRequestType;

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
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anySet;
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
    private PrivacyEnforcementService privacyEnforcementService;
    @Mock
    private TcfDefinerService tcfDefinerService;
    @Mock
    private AnalyticsReporterDelegator analyticsReporterDelegator;
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

    private TcfContext tcfContext;

    @Before
    public void setUp() {
        final Map<Integer, PrivacyEnforcementAction> vendorIdToGdpr = singletonMap(1,
                PrivacyEnforcementAction.allowAll());

        tcfContext = TcfContext.builder().gdpr("GDPR").build();
        given(privacyEnforcementService.contextFromSetuidRequest(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(PrivacyContext.of(null, tcfContext)));
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
                .willReturn(Future.succeededFuture(TcfResponse.of(true, vendorIdToGdpr, null)));

        given(routingContext.request()).willReturn(httpRequest);
        given(routingContext.response()).willReturn(httpResponse);

        given(httpResponse.headers()).willReturn(new CaseInsensitiveHeaders());
        given(httpResponse.putHeader(any(CharSequence.class), any(CharSequence.class))).willReturn(httpResponse);

        given(uidsCookieService.toCookie(any())).willReturn(Cookie.cookie("test", "test"));

        given(applicationSettings.getAccountById(any(), any())).willReturn(Future.failedFuture("not found"));

        given(bidderCatalog.names()).willReturn(new HashSet<>(asList("rubicon", "appnexus", "audienceNetwork")));
        given(bidderCatalog.isActive(any())).willReturn(true);

        given(bidderCatalog.usersyncerByName(eq(RUBICON))).willReturn(
                Usersyncer.of(RUBICON, Usersyncer.UsersyncMethod.of("redirect", null, null, false), null));
        given(bidderCatalog.usersyncerByName(eq(FACEBOOK))).willReturn(
                Usersyncer.of(FACEBOOK, Usersyncer.UsersyncMethod.of("redirect", null, null, false), null));
        given(bidderCatalog.usersyncerByName("appnexus")).willReturn(
                Usersyncer.of(ADNXS, Usersyncer.UsersyncMethod.of("iframe", null, null, false), null));

        given(httpRequest.getParam("account")).willReturn("account");
        given(httpRequest.headers()).willReturn(new CaseInsensitiveHeaders());
        given(httpRequest.remoteAddress()).willReturn(new SocketAddressImpl(80, "localhost"));
        given(httpResponse.headers()).willReturn(responseHeaders);

        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(
                2000,
                uidsCookieService,
                applicationSettings,
                bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                1,
                analyticsReporterDelegator,
                metrics,
                timeoutFactory,
                true,
                uidsAuditCookieService);
    }

    @Test
    public void shouldRespondWithEmptyBodyAndNoContentStatusIfCookieIsDisabled() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(2000, uidsCookieService, applicationSettings, bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                null,
                analyticsReporterDelegator,
                metrics,
                timeoutFactory, false,
                uidsAuditCookieService);
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(204));
        verify(httpResponse).end();
    }

    @Test
    public void shouldRespondWithErrorAndTriggerMetricsAndAnalyticsWhenOptedOut() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).optout(true).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(401));
        verify(httpResponse).end("Unauthorized: Sync is not allowed for this uids");
        verify(metrics).updateUserSyncOptoutMetric();

        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.error(401));
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
        verify(httpResponse).end(eq("Invalid request format: \"bidder\" query param is required"));
        verify(metrics).updateUserSyncBadRequestMetric();

        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.error(400));
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

        given(httpRequest.getParam(eq("bidder"))).willReturn("invalid_or_disabled");
        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end(eq("Invalid request format: \"bidder\" query param is invalid"));
        verify(metrics).updateUserSyncBadRequestMetric();
    }

    @Test
    public void shouldRespondWithBadRequestStatusIfGdprConsentIsInvalid() {
        // given
        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        tcfContext = TcfContext.builder().gdpr("1").isConsentValid(false).build();
        given(privacyEnforcementService.contextFromSetuidRequest(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(PrivacyContext.of(null, tcfContext)));

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(metrics).updateUserSyncTcfInvalidMetric(RUBICON);
        verify(httpResponse).setStatusCode(eq(400));
        verify(httpResponse).end(eq("Invalid request format: Consent string is invalid"));
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
    public void shouldRespondWithoutCookieIfGdprProcessingPreventsCookieSetting() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        setuidHandler = new SetuidHandler(
                2000,
                uidsCookieService,
                applicationSettings,
                bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                1,
                analyticsReporterDelegator,
                metrics,
                new TimeoutFactory(clock),
                true,
                uidsAuditCookieService);
        final PrivacyEnforcementAction privacyEnforcementAction = PrivacyEnforcementAction.restrictAll();
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
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
        verify(httpResponse).setStatusCode(eq(451));
        verify(httpResponse).end(eq("The gdpr_consent param prevents cookies from being saved"));
        verify(metrics).updateUserSyncTcfBlockedMetric(RUBICON);

        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.builder().status(451).build());
    }

    @Test
    public void shouldRespondWithBadRequestStatusIfGdprProcessingFailsWithInvalidRequestException() {
        // given
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
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
        verify(httpResponse).end(eq("Invalid request format: gdpr exception"));

        final SetuidEvent setuidEvent = captureSetuidEvent();
        assertThat(setuidEvent).isEqualTo(SetuidEvent.error(400));
    }

    @Test
    public void shouldRespondWithInternalServerErrorStatusIfGdprProcessingFailsWithUnexpectedException() {
        // given
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
                .willReturn(Future.failedFuture("unexpected error TCF"));

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
        verify(httpResponse).end(eq("Unexpected setuid processing error: unexpected error TCF"));
    }

    @Test
    public void shouldPassAccountToPrivacyEnforcementServiceWhenAccountIsFound() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("account")).willReturn("accId");

        final AccountGdprConfig accountGdprConfig = AccountGdprConfig.builder()
                .enabledForRequestType(EnabledForRequestType.of(true, true, true, true))
                .build();
        final Account account = Account.builder().gdpr(accountGdprConfig).build();
        final Future<Account> accountFuture = Future.succeededFuture(account);
        given(applicationSettings.getAccountById(any(), any())).willReturn(accountFuture);

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(applicationSettings).getAccountById(eq("accId"), any());
        verify(privacyEnforcementService).contextFromSetuidRequest(any(), eq(account), any(), any());
    }

    @Test
    public void shouldPassAccountToPrivacyEnforcementServiceWhenAccountIsNotFound() {
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
        verify(privacyEnforcementService).contextFromSetuidRequest(any(), eq(Account.empty("accId")), any(), any());
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
        given(httpRequest.getParam("f")).willReturn("i");

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
        given(bidderCatalog.names()).willReturn(singleton(FACEBOOK));

        given(uidsAuditCookieService.getUidsAudit(any(RoutingContext.class))).willReturn(UidAudit.builder().build());
        given(uidsAuditCookieService.createUidsAuditCookie(any(), any(), any(), any(), any(), any()))
                .willReturn(Cookie.cookie("uids-audit", "value").setDomain("rubicon"));

        given(bidderCatalog.usersyncerByName(any())).willReturn(
                Usersyncer.of(FACEBOOK, Usersyncer.UsersyncMethod.of("iframe", null, null, false), null));

        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(
                2000,
                uidsCookieService,
                applicationSettings,
                bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                1,
                analyticsReporterDelegator,
                metrics,
                timeoutFactory, true,
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
        given(httpRequest.getParam("uid")).willReturn("J5VLCWQP-26-CWFT");

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(RUBICON).getUid()).isEqualTo("J5VLCWQP-26-CWFT");
    }

    @Test
    public void shouldSendPixelWhenFParamIsEqualToIWhenTypeIsIframe() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        // {"tempUIDs":{"rubicon":{"uid":"J5VLCWQP-26-CWFT"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJydWJpY29uIjp7InVpZCI6Iko1VkxDV1FQLTI2LUNXRlQifX19"));
        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("f")).willReturn("i");
        given(httpRequest.getParam("uid")).willReturn("J5VLCWQP-26-CWFT");

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).sendFile(any());
    }

    @Test
    public void shouldSendEmptyResponseWhenFParamIsEqualToBWhenTypeIsRedirect() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        // {"tempUIDs":{"rubicon":{"uid":"J5VLCWQP-26-CWFT"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJydWJpY29uIjp7InVpZCI6Iko1VkxDV1FQLTI2LUNXRlQifX19"));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("f")).willReturn("b");
        given(httpRequest.getParam("uid")).willReturn("J5VLCWQP-26-CWFT");
        given(bidderCatalog.names()).willReturn(singleton(RUBICON));
        given(bidderCatalog.usersyncerByName(any()))
                .willReturn(Usersyncer.of(RUBICON, Usersyncer.UsersyncMethod.of("redirect", null, null, false), null));

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);
        setuidHandler = new SetuidHandler(
                2000,
                uidsCookieService,
                applicationSettings,
                bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                null,
                analyticsReporterDelegator,
                metrics,
                new TimeoutFactory(Clock.fixed(Instant.now(), ZoneId.systemDefault())),
                true,
                uidsAuditCookieService);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse, never()).sendFile(any());
        verify(httpResponse).putHeader(eq(HttpHeaders.CONTENT_LENGTH), eq("0"));
        verify(httpResponse).putHeader(eq(HttpHeaders.CONTENT_TYPE), eq(HttpHeaders.TEXT_HTML));
    }

    @Test
    public void shouldSendEmptyResponseWhenFParamNotDefinedAndTypeIsIframe() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        // {"tempUIDs":{"rubicon":{"uid":"J5VLCWQP-26-CWFT"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJydWJpY29uIjp7InVpZCI6Iko1VkxDV1FQLTI2LUNXRlQifX19"));

        given(bidderCatalog.usersyncerByName(eq(RUBICON))).willReturn(
                Usersyncer.of(RUBICON, Usersyncer.UsersyncMethod.of("iframe", null, null, false), null));

        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(httpRequest.getParam("uid")).willReturn("J5VLCWQP-26-CWFT");

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        setuidHandler = new SetuidHandler(
                2000,
                uidsCookieService,
                applicationSettings,
                bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                null,
                analyticsReporterDelegator,
                metrics,
                new TimeoutFactory(Clock.fixed(Instant.now(), ZoneId.systemDefault())),
                true,
                uidsAuditCookieService);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse, never()).sendFile(any());
        verify(httpResponse).putHeader(eq(HttpHeaders.CONTENT_LENGTH), eq("0"));
        verify(httpResponse).putHeader(eq(HttpHeaders.CONTENT_TYPE), eq(HttpHeaders.TEXT_HTML));
    }

    @Test
    public void shouldSendPixelWhenFParamNotDefinedAndTypeIsRedirect() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        // {"tempUIDs":{"rubicon":{"uid":"J5VLCWQP-26-CWFT"}}}
        given(uidsCookieService.toCookie(any())).willReturn(Cookie
                .cookie("uids", "eyJ0ZW1wVUlEcyI6eyJydWJpY29uIjp7InVpZCI6Iko1VkxDV1FQLTI2LUNXRlQifX19"));
        given(httpRequest.getParam("bidder")).willReturn(RUBICON);
        given(bidderCatalog.names()).willReturn(singleton(RUBICON));
        given(bidderCatalog.usersyncerByName(any()))
                .willReturn(Usersyncer.of(RUBICON, Usersyncer.UsersyncMethod.of("redirect", null, null, false), null));
        given(httpRequest.getParam("uid")).willReturn("J5VLCWQP-26-CWFT");

        given(httpResponse.setStatusCode(anyInt())).willReturn(httpResponse);

        setuidHandler = new SetuidHandler(
                2000,
                uidsCookieService,
                applicationSettings,
                bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                null,
                analyticsReporterDelegator,
                metrics,
                new TimeoutFactory(Clock.fixed(Instant.now(), ZoneId.systemDefault())),
                true,
                uidsAuditCookieService);

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(routingContext, never()).addCookie(any(Cookie.class));
        verify(httpResponse).sendFile(any());
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
        given(uidsCookieService.toCookie(any()))
                .willReturn(Cookie.cookie("uids",
                        "eyJ0ZW1wVUlEcyI6eyJhZG54cyI6eyJ1aWQiOiIxMjM0NSJ9LCAicnViaWNvbiI6eyJ1aWQiOiJ1cGRhdGVkVW"
                                + "lkIn19fQ=="));

        // when
        setuidHandler.handle(routingContext);

        // then
        verify(httpResponse).sendFile(any());
        verify(routingContext, never()).addCookie(any(Cookie.class));

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(2);
        assertThat(decodedUids.getUids().get(RUBICON).getUid()).isEqualTo("updatedUid");
        assertThat(decodedUids.getUids().get(ADNXS).getUid()).isEqualTo("12345");
    }

    @Test
    public void shouldRespondWithCookieIfVednorHostDefinedUserIsNotInGdprScope() throws IOException {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        setuidHandler = new SetuidHandler(
                2000,
                uidsCookieService,
                applicationSettings,
                bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                1,
                analyticsReporterDelegator,
                metrics,
                new TimeoutFactory(clock),
                true,
                uidsAuditCookieService);
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
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
        verify(httpResponse).sendFile(any());

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(RUBICON).getUid()).isEqualTo("J5VLCWQP-26-CWFT");
    }

    @Test
    public void shouldSkipTcfChecksAndRespondWithCookieIfHostVendorIdNotDefined() throws IOException {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        setuidHandler = new SetuidHandler(2000, uidsCookieService, applicationSettings,
                bidderCatalog, privacyEnforcementService, tcfDefinerService, null, analyticsReporterDelegator, metrics,
                new TimeoutFactory(clock), true, uidsAuditCookieService);
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
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
        verify(httpResponse).sendFile(any());

        final String uidsCookie = captureCookie();
        final Uids decodedUids = decodeUids(uidsCookie);
        assertThat(decodedUids.getUids()).hasSize(1);
        assertThat(decodedUids.getUids().get(RUBICON).getUid()).isEqualTo("J5VLCWQP-26-CWFT");
    }

    @Test
    public void shouldRespondNotPreventsCookieSettingWhenGdprHostVendorIsNotDefinedAndUserIsInGdprScope()
            throws IOException {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        setuidHandler = new SetuidHandler(2000, uidsCookieService, applicationSettings,
                bidderCatalog, privacyEnforcementService, tcfDefinerService, null, analyticsReporterDelegator, metrics,
                new TimeoutFactory(clock), true, uidsAuditCookieService);
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
                .willReturn(Future.succeededFuture(TcfResponse.of(true, emptyMap(), null)));

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
        verify(httpResponse).sendFile(any());

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
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
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
        given(tcfDefinerService.resultForVendorIds(anySet(), any()))
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
    public void shouldPassUnsuccessfulEventToAnalyticsReporterIfFacebookSentinel() {
        // given
        given(uidsCookieService.parseFromRequest(any()))
                .willReturn(new UidsCookie(Uids.builder().uids(emptyMap()).build(), jacksonMapper));

        given(httpRequest.getParam("bidder")).willReturn(FACEBOOK);
        given(httpRequest.getParam("uid")).willReturn("0");

        given(uidsAuditCookieService.getUidsAudit(any(RoutingContext.class))).willReturn(UidAudit.builder().build());
        given(uidsAuditCookieService.createUidsAuditCookie(any(), any(), any(), any(), any(), any()))
                .willReturn(Cookie.cookie("uids-audit", "value").setDomain("rubicon"));
        given(bidderCatalog.names()).willReturn(singleton(FACEBOOK));

        given(bidderCatalog.usersyncerByName(any())).willReturn(
                Usersyncer.of(FACEBOOK, Usersyncer.UsersyncMethod.of("redirect", null, null, false), null));

        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        setuidHandler = new SetuidHandler(
                2000,
                uidsCookieService,
                applicationSettings,
                bidderCatalog,
                privacyEnforcementService,
                tcfDefinerService,
                null,
                analyticsReporterDelegator,
                metrics,
                timeoutFactory,
                true,
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
        verify(analyticsReporterDelegator).processEvent(setuidEventCaptor.capture(), eq(tcfContext));
        return setuidEventCaptor.getValue();
    }
}
