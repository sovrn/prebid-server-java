package org.prebid.server.rubicon.analytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Device;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Geo;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.VertxTest;
import org.prebid.server.analytics.model.AmpEvent;
import org.prebid.server.analytics.model.AuctionEvent;
import org.prebid.server.analytics.model.HttpContext;
import org.prebid.server.analytics.model.NotificationEvent;
import org.prebid.server.auction.IpAddressHelper;
import org.prebid.server.auction.model.AuctionContext;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.bidder.Usersyncer;
import org.prebid.server.cookie.UidsCookie;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.cookie.model.UidWithExpiry;
import org.prebid.server.cookie.proto.Uids;
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.exception.InvalidRequestException;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.geolocation.model.GeoInfo;
import org.prebid.server.privacy.gdpr.model.TcfContext;
import org.prebid.server.privacy.model.PrivacyContext;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtApp;
import org.prebid.server.proto.openrtb.ext.request.ExtAppPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtImpContext;
import org.prebid.server.proto.openrtb.ext.request.ExtRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidChannel;
import org.prebid.server.proto.openrtb.ext.request.ExtStoredRequest;
import org.prebid.server.proto.openrtb.ext.request.rubicon.ExtImpRubicon;
import org.prebid.server.proto.openrtb.ext.request.rubicon.RubiconVideoParams;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponse;
import org.prebid.server.proto.openrtb.ext.response.ExtBidderError;
import org.prebid.server.rubicon.analytics.proto.AdUnit;
import org.prebid.server.rubicon.analytics.proto.Auction;
import org.prebid.server.rubicon.analytics.proto.BidError;
import org.prebid.server.rubicon.analytics.proto.BidWon;
import org.prebid.server.rubicon.analytics.proto.Client;
import org.prebid.server.rubicon.analytics.proto.Dimensions;
import org.prebid.server.rubicon.analytics.proto.Error;
import org.prebid.server.rubicon.analytics.proto.Event;
import org.prebid.server.rubicon.analytics.proto.EventCreator;
import org.prebid.server.rubicon.analytics.proto.Gam;
import org.prebid.server.rubicon.analytics.proto.Gdpr;
import org.prebid.server.rubicon.analytics.proto.Impression;
import org.prebid.server.rubicon.analytics.proto.Params;
import org.prebid.server.rubicon.analytics.proto.User;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.rubicon.audit.proto.UidAudit;
import org.prebid.server.rubicon.proto.request.ExtRequestPrebidBidders;
import org.prebid.server.rubicon.proto.request.ExtRequestPrebidBiddersRubicon;
import org.prebid.server.settings.model.Account;
import org.prebid.server.settings.model.AccountAnalyticsConfig;
import org.prebid.server.vertx.http.HttpClient;
import org.prebid.server.vertx.http.model.HttpClientResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.within;
import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class RubiconAnalyticsReporterTest extends VertxTest {

    private static final String HOST_URL = "http://host-url";
    private static final int PBS_HOST_VENDOR_ID = 52;
    private static final String ADAPTER_NAME = "rubicon";

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private BidderCatalog bidderCatalog;
    @Mock
    private UidsCookieService uidsCookieService;
    @Mock
    private UidsAuditCookieService uidsAuditCookieService;
    @Mock
    private CurrencyConversionService currencyService;
    @Mock
    private IpAddressHelper ipAddressHelper;
    @Mock
    private UidsCookie uidsCookie;
    @Mock
    private HttpClient httpClient;

    private RubiconAnalyticsReporter reporter;
    @Mock
    private RoutingContext routingContext;
    @Mock
    private HttpServerRequest httpRequest;

    private HttpContext httpContext;

    @Before
    public void setUp() {
        given(bidderCatalog.isValidName("rubicon")).willReturn(true);
        given(bidderCatalog.isValidName("appnexus")).willReturn(true);
        given(bidderCatalog.usersyncerByName("rubicon")).willReturn(Usersyncer.of("rubicon", null, null));
        given(bidderCatalog.usersyncerByName("appnexus")).willReturn(Usersyncer.of("appnexus", null, null));

        given(uidsCookie.hasLiveUidFrom("rubicon")).willReturn(true);
        given(uidsCookie.hasLiveUidFrom("appnexus")).willReturn(false);

        given(uidsCookieService.parseHostCookie(any())).willReturn("khaos-cookie-value");
        given(uidsCookieService.parseFromCookies(any()))
                .willReturn(new UidsCookie(Uids.builder()
                        .uids(singletonMap("rubicon", UidWithExpiry.live("uid")))
                        .build(), jacksonMapper));

        given(uidsAuditCookieService.getUidsAudit(anyMap()))
                .willReturn(UidAudit.builder().country("countryFromAuditCookie").build());

        given(currencyService.convertCurrency(any(), any(), any(), any(), any()))
                .willAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

        given(routingContext.request()).willReturn(httpRequest);
        given(httpRequest.uri()).willReturn("");
        given(httpRequest.params()).willReturn(MultiMap.caseInsensitiveMultiMap());
        given(httpRequest.headers()).willReturn(new CaseInsensitiveHeaders());

        httpContext = HttpContext.builder().cookies(emptyMap()).build();

        reporter = new RubiconAnalyticsReporter(HOST_URL, 1, "pbs-version-1", "pbsHostname", PBS_HOST_VENDOR_ID,
                "dataCenterRegion", bidderCatalog, uidsCookieService, uidsAuditCookieService, currencyService,
                ipAddressHelper, httpClient, false, jacksonMapper);
    }

    @Test
    public void creationShouldFailOnInvalidEndpointUrl() {
        assertThatIllegalArgumentException()
                .isThrownBy(
                        () -> new RubiconAnalyticsReporter("invalid_url", null, null, null, PBS_HOST_VENDOR_ID, null,
                                null, null, null, null, null, null, false, null))
                .withMessage("URL supplied is not valid: invalid_url/event");
    }

    @Test
    public void vendorIdShouldReturnPassedVendorId() {
        // expected
        assertThat(reporter.vendorId()).isEqualTo(PBS_HOST_VENDOR_ID);
    }

    @Test
    public void nameShouldReturnAnalyticCode() {
        // expected
        assertThat(reporter.name()).isEqualTo(ADAPTER_NAME);
    }

    @Test
    public void processAuctionEventShouldIgnoreProcessingIfAuctionContextIsMissing() {
        // given
        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(null)
                .build();

        // when
        final Future<Void> future = reporter.processEvent(auctionEvent);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(InvalidRequestException.class)
                .hasMessage("Exception is thrown while auction processing");
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAuctionEventShouldIgnoreProcessingIfHttpContextIsMissing() {
        // given
        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder().build())
                .httpContext(null)
                .build();

        // when
        final Future<Void> future = reporter.processEvent(auctionEvent);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(InvalidRequestException.class)
                .hasMessage("Necessary data is missing while auction processing");
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAuctionEventShouldIgnoreProcessingIfBidRequestIsMissing() {
        // given
        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(null)
                        .build())
                .httpContext(HttpContext.builder().build())
                .build();

        // when
        final Future<Void> future = reporter.processEvent(auctionEvent);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(InvalidRequestException.class)
                .hasMessage("Necessary data is missing while auction processing");
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAuctionEventShouldIgnoreProcessingIfAccountIsMissing() {
        // given
        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder().build())
                        .account(null)
                        .build())
                .httpContext(HttpContext.builder().build())
                .build();

        // when
        final Future<Void> future = reporter.processEvent(auctionEvent);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(InvalidRequestException.class)
                .hasMessage("Necessary data is missing while auction processing");
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAuctionEventShouldIgnoreProcessingIfBidResponseIsMissing() {
        // given
        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder().build())
                        .account(Account.builder().build())
                        .build())
                .httpContext(HttpContext.builder().build())
                .bidResponse(null)
                .build();

        // when
        final Future<Void> future = reporter.processEvent(auctionEvent);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(InvalidRequestException.class)
                .hasMessage("Necessary data is missing while auction processing");
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processEventShouldIgnoreNonMobileRequests() {
        // given
        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(givenAuctionContext(
                        BidRequest.builder()
                                .site(Site.builder().build())
                                .build()))
                .build();

        // when
        final Future<Void> future = reporter.processEvent(auctionEvent);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(InvalidRequestException.class)
                .hasMessage("Necessary data is missing while auction processing");
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processEventShouldRespondWithErrorIfHttpClientFails() {
        // given
        givenHttpClientProducesException(new RuntimeException("error"));

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .httpContext(httpContext)
                .auctionContext(givenAuctionContext(sampleAuctionBidRequest(null, null)))
                .bidResponse(sampleBidResponse())
                .build();

        // when
        final Future<Void> future = reporter.processEvent(auctionEvent);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(RuntimeException.class).hasMessage("error");
    }

    @Test
    public void processEventShouldRespondWithErrorIfHttpClientRespondsWithNot200Status() {
        // given
        givenHttpClientReturnsResponse(500, null);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .httpContext(httpContext)
                .auctionContext(givenAuctionContext(sampleAuctionBidRequest(null, null)))
                .bidResponse(sampleBidResponse())
                .build();

        // when
        final Future<Void> future = reporter.processEvent(auctionEvent);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(PreBidException.class).hasMessage("HTTP status code 500");
    }

    @Test
    public void processAuctionEventShouldProcessWebRequestsIfAllowedByAccount() {
        // given
        givenHttpClientReturnsResponse(200, null);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder()
                                .imp(emptyList())
                                .cur(singletonList("USD"))
                                .site(Site.builder().build())
                                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                        .channel(ExtRequestPrebidChannel.of("web"))
                                        .build()))
                                .build())
                        .account(Account.builder()
                                .analytics(AccountAnalyticsConfig.of(singletonMap("web", true), null))
                                .build())
                        .privacyContext(PrivacyContext.of(null, TcfContext.empty()))
                        .build())
                .httpContext(httpContext)
                .bidResponse(BidResponse.builder()
                        .seatbid(emptyList())
                        .build())
                .build();

        // when
        reporter.processEvent(auctionEvent);

        // then
        verify(httpClient).post(anyString(), any(), any(), anyLong());
    }

    @Test
    public void processAuctionEventShouldNotProcessClientAnalyticsRequest() {
        // given
        givenHttpClientReturnsResponse(200, null);

        final ObjectNode analyticNode = mapper.createObjectNode();
        final ObjectNode rubiconNode = mapper.createObjectNode();
        rubiconNode.set("client-analytics", BooleanNode.valueOf(true));
        analyticNode.set(reporter.name(), rubiconNode);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder()
                                .imp(emptyList())
                                .cur(singletonList("USD"))
                                .site(Site.builder().build())
                                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                        .channel(ExtRequestPrebidChannel.of("web"))
                                        .analytics(analyticNode)
                                        .build()))
                                .build())
                        .account(Account.builder()
                                .analytics(AccountAnalyticsConfig.of(singletonMap("web", true), null))
                                .build())
                        .privacyContext(PrivacyContext.of(null, TcfContext.empty()))
                        .build())
                .httpContext(httpContext)
                .bidResponse(BidResponse.builder()
                        .seatbid(emptyList())
                        .build())
                .build();

        // when
        reporter.processEvent(auctionEvent);

        // then
        verifyZeroInteractions(httpClient);
    }

    @Test
    public void processAuctionEventShouldProcessNotClientAnalyticsRequest() {
        // given
        givenHttpClientReturnsResponse(200, null);

        final ObjectNode analyticNode = mapper.createObjectNode();
        final ObjectNode rubiconNode = mapper.createObjectNode();
        rubiconNode.set("client-analytics", BooleanNode.valueOf(false));
        analyticNode.set(reporter.name(), rubiconNode);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder()
                                .imp(emptyList())
                                .cur(singletonList("USD"))
                                .site(Site.builder().build())
                                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                        .channel(ExtRequestPrebidChannel.of("web"))
                                        .analytics(analyticNode)
                                        .build()))
                                .build())
                        .account(Account.builder()
                                .analytics(AccountAnalyticsConfig.of(singletonMap("web", true), null))
                                .build())
                        .privacyContext(PrivacyContext.of(null, TcfContext.empty()))
                        .build())
                .httpContext(httpContext)
                .bidResponse(BidResponse.builder()
                        .seatbid(emptyList())
                        .build())
                .build();

        // when
        reporter.processEvent(auctionEvent);

        // then
        verify(httpClient).post(anyString(), any(), any(), anyLong());
    }

    @Test
    public void processAuctionEventShouldTolerateMissingExtPrebidBidderNode() {
        // given
        givenHttpClientReturnsResponse(200, null);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder()
                                .imp(singletonList(Imp.builder()
                                        .id("impId")
                                        .ext(mapper.createObjectNode().set("prebid", mapper.createObjectNode()))
                                        .video(Video.builder().build())
                                        .build()))
                                .cur(singletonList("USD"))
                                .site(Site.builder().build())
                                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                        .channel(ExtRequestPrebidChannel.of("web"))
                                        .build()))
                                .build())
                        .account(Account.builder()
                                .analytics(AccountAnalyticsConfig.of(singletonMap("web", true), null))
                                .build())
                        .privacyContext(PrivacyContext.of(null, TcfContext.empty()))
                        .build())
                .httpContext(httpContext)
                .bidResponse(BidResponse.builder()
                        .seatbid(singletonList(SeatBid.builder()
                                .seat("rubicon")
                                .bid(singletonList(Bid.builder()
                                        .impid("impId")
                                        .ext(mapper.createObjectNode()
                                                .set("prebid", mapper.createObjectNode()
                                                        .put("type", "video")))
                                        .build()))
                                .build()))
                        .build())
                .build();

        // when
        reporter.processEvent(auctionEvent);

        // then
        verify(httpClient).post(anyString(), any(), any(), anyLong());
    }

    @Test
    public void processAuctionEventShouldPreferBidIdFromExtPrebidOverRootBidId() throws JsonProcessingException {
        // given
        givenHttpClientReturnsResponse(200, null);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder()
                                .imp(singletonList(Imp.builder().id("impId").build()))
                                .cur(singletonList("USD"))
                                .site(Site.builder().build())
                                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                        .channel(ExtRequestPrebidChannel.of("web"))
                                        .build()))
                                .build())
                        .account(Account.builder()
                                .analytics(AccountAnalyticsConfig.of(singletonMap("web", true), null))
                                .build())
                        .privacyContext(PrivacyContext.of(null, TcfContext.empty()))
                        .build())
                .httpContext(httpContext)
                .bidResponse(BidResponse.builder()
                        .seatbid(singletonList(SeatBid.builder()
                                .bid(singletonList(Bid.builder()
                                        .impid("impId")
                                        .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.builder()
                                                        .bidid("generatedId")
                                                        .build(),
                                                null)))
                                        .build()))
                                .build()))
                        .build())
                .build();

        // when
        reporter.processEvent(auctionEvent);

        // then
        final ArgumentCaptor<String> bodyArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(anyString(), any(), bodyArgumentCaptor.capture(), anyLong());
        final Event result = mapper.readValue(bodyArgumentCaptor.getValue(), Event.class);
        assertThat(result.getAuctions())
                .hasSize(1)
                .flatExtracting(Auction::getAdUnits)
                .flatExtracting(AdUnit::getBids)
                .flatExtracting(org.prebid.server.rubicon.analytics.proto.Bid::getBidId)
                .containsOnly("generatedId");
    }

    @Test
    public void processAuctionEventShouldSetBidderDetailIfTargetBidderCodeIsPresent() throws JsonProcessingException {
        // given
        givenHttpClientReturnsResponse(200, null);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder()
                                .imp(singletonList(Imp.builder().id("impId").build()))
                                .cur(singletonList("USD"))
                                .site(Site.builder().build())
                                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                        .channel(ExtRequestPrebidChannel.of("web"))
                                        .build()))
                                .build())
                        .account(Account.builder()
                                .analytics(AccountAnalyticsConfig.of(singletonMap("web", true), null))
                                .build())
                        .privacyContext(PrivacyContext.of(null, TcfContext.empty()))
                        .build())
                .httpContext(httpContext)
                .bidResponse(BidResponse.builder()
                        .seatbid(singletonList(SeatBid.builder()
                                .seat("seatValue")
                                .bid(singletonList(Bid.builder()
                                        .impid("impId")
                                        .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.builder()
                                                        .targetBidderCode("targetCode")
                                                        .bidid("generatedId")
                                                        .build(),
                                                null)))
                                        .build()))
                                .build()))
                        .build())
                .build();

        // when
        reporter.processEvent(auctionEvent);

        // then
        final ArgumentCaptor<String> bodyArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(anyString(), any(), bodyArgumentCaptor.capture(), anyLong());
        final Event result = mapper.readValue(bodyArgumentCaptor.getValue(), Event.class);
        assertThat(result.getAuctions())
                .hasSize(1)
                .flatExtracting(Auction::getAdUnits)
                .flatExtracting(AdUnit::getBids)
                .flatExtracting(org.prebid.server.rubicon.analytics.proto.Bid::getBidderDetail)
                .containsOnly("targetCode");
    }

    @Test
    public void processAuctionEventShouldNotUpdateBidderDetailsIfTargetCodeIsBlank() throws JsonProcessingException {
        // given
        givenHttpClientReturnsResponse(200, null);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder()
                                .imp(singletonList(Imp.builder().id("impId").build()))
                                .cur(singletonList("USD"))
                                .site(Site.builder().build())
                                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                        .channel(ExtRequestPrebidChannel.of("web"))
                                        .build()))
                                .build())
                        .account(Account.builder()
                                .analytics(AccountAnalyticsConfig.of(singletonMap("web", true), null))
                                .build())
                        .privacyContext(PrivacyContext.of(null, TcfContext.empty()))
                        .build())
                .httpContext(httpContext)
                .bidResponse(BidResponse.builder()
                        .seatbid(singletonList(SeatBid.builder()
                                .seat("seatValue")
                                .bid(singletonList(Bid.builder()
                                        .impid("impId")
                                        .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.builder()
                                                        .targetBidderCode("  ")
                                                        .bidid("generatedId")
                                                        .build(),
                                                null)))
                                        .build()))
                                .build()))
                        .build())
                .build();

        // when
        reporter.processEvent(auctionEvent);

        // then
        final ArgumentCaptor<String> bodyArgumentCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(anyString(), any(), bodyArgumentCaptor.capture(), anyLong());
        final Event result = mapper.readValue(bodyArgumentCaptor.getValue(), Event.class);
        assertThat(result.getAuctions())
                .hasSize(1)
                .flatExtracting(Auction::getAdUnits)
                .flatExtracting(AdUnit::getBids)
                .flatExtracting(org.prebid.server.rubicon.analytics.proto.Bid::getBidderDetail)
                .containsNull();
    }

    @Test
    public void processAuctionEventShouldUseGlobalSamplingFactor() {
        // given
        reporter = new RubiconAnalyticsReporter(HOST_URL, 10, "pbs-version-1", "pbsHostname", PBS_HOST_VENDOR_ID,
                "dataCenterRegion", bidderCatalog, uidsCookieService, uidsAuditCookieService,
                currencyService, ipAddressHelper, httpClient, false, jacksonMapper);

        givenHttpClientReturnsResponse(200, null);

        final AuctionContext auctionContext = givenAuctionContext(
                BidRequest.builder()
                        .imp(emptyList())
                        .app(App.builder().build())
                        .cur(singletonList("USD"))
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .channel(ExtRequestPrebidChannel.of("app"))
                                .build()))
                        .build());

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .httpContext(httpContext)
                .auctionContext(auctionContext)
                .bidResponse(BidResponse.builder()
                        .seatbid(emptyList())
                        .build())
                .build();

        // when
        for (int i = 0; i < 10; i++) {
            reporter.processEvent(auctionEvent);
        }

        // then
        verify(httpClient).post(anyString(), any(), any(), anyLong());
    }

    @Test
    public void processAuctionEventShouldUseAccountSamplingFactorOverGlobal() {
        // given
        reporter = new RubiconAnalyticsReporter(HOST_URL, 100, "pbs-version-1", "pbsHostname", PBS_HOST_VENDOR_ID,
                "dataCenterRegion", bidderCatalog, uidsCookieService, uidsAuditCookieService,
                currencyService, ipAddressHelper, httpClient, false, jacksonMapper);

        givenHttpClientReturnsResponse(200, null);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .httpContext(httpContext)
                .auctionContext(givenAuctionContext(sampleAuctionBidRequest(null, null), 10))
                .bidResponse(sampleBidResponse())
                .build();

        // when
        for (int i = 0; i < 10; i++) {
            reporter.processEvent(auctionEvent);
        }

        // then
        verify(httpClient).post(anyString(), any(), any(), anyLong());
    }

    @Test
    public void processAuctionEventShouldPostEventToEndpointWithExpectedHeaders() {
        // given
        givenHttpClientReturnsResponse(200, null);

        given(ipAddressHelper.maskIpv4(anyString())).willReturn("masked-ip");

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .httpContext(httpContext)
                .auctionContext(givenAuctionContext(sampleAuctionBidRequest(null, null)))
                .bidResponse(sampleBidResponse())
                .build();

        // when
        reporter.processEvent(auctionEvent);

        // then
        verify(ipAddressHelper).maskIpv4(eq("104.22.41.73"));
        final ArgumentCaptor<MultiMap> captor = ArgumentCaptor.forClass(MultiMap.class);
        verify(httpClient).post(anyString(), captor.capture(), any(), anyLong());

        assertThat(captor.getValue().entries())
                .extracting(Map.Entry::getKey, Map.Entry::getValue)
                .containsOnly(
                        tuple("Content-Type", "application/json;charset=utf-8"),
                        tuple("Referer", "refererPage"),
                        tuple("User-Agent", "userAgent"),
                        tuple("X-Forwarded-For", "masked-ip"));
    }

    @SuppressWarnings("checkstyle:methodlength")
    @Test
    public void processAuctionEventShouldPostEventToEndpointWithExpectedBody() throws IOException {
        // given
        givenHttpClientReturnsResponse(200, null);
        given(bidderCatalog.isValidName("unknown")).willReturn(false);
        final String integration = "dbpg";
        final String wrappername = "12314wp";

        final AuctionEvent event = AuctionEvent.builder()
                .httpContext(httpContext)
                .auctionContext(givenAuctionContext(sampleAuctionBidRequest(integration, wrappername)))
                .bidResponse(sampleBidResponse())
                .build();

        // when
        reporter.processEvent(event);

        // then
        final ArgumentCaptor<String> eventCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(eq("http://host-url/event"), any(), eventCaptor.capture(), anyLong());

        then(mapper.readValue(eventCaptor.getValue(), Event.class)).isEqualTo(expectedEventBuilderBaseFromApp()
                .auctions(singletonList(Auction.of("bidRequestId", 1,
                        asList(
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId1")
                                        .status("success")
                                        .mediaTypes(asList("banner", "video"))
                                        .videoAdFormat("interstitial")
                                        .dimensions(asList(Dimensions.of(200, 300), Dimensions.of(300, 400)))
                                        .adserverTargeting(singletonMap("key1", "value1"))
                                        .siteId(456)
                                        .zoneId(789)
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidId("bidId1")
                                                        .bidder("rubicon")
                                                        .status("success")
                                                        .source("server")
                                                        .serverLatencyMillis(101)
                                                        .serverHasUserId(true)
                                                        .params(Params.of(123, 456, 789))
                                                        .bidResponse(org.prebid.server.rubicon.analytics.proto
                                                                .BidResponse.of(345, BigDecimal.valueOf(4.56),
                                                                        "video", Dimensions.of(500, 600)))
                                                        .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId2")
                                        .status("success")
                                        .mediaTypes(singletonList("video"))
                                        .videoAdFormat("mid-roll")
                                        .dimensions(singletonList(Dimensions.of(100, 200)))
                                        .adserverTargeting(singletonMap("key22", "value22"))
                                        .bids(asList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidId("bidId2")
                                                        .bidder("appnexus")
                                                        .status("success")
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .bidResponse(org.prebid.server.rubicon.analytics.proto
                                                                .BidResponse.of(456, BigDecimal.valueOf(5.67),
                                                                        "video", Dimensions.of(600, 700)))
                                                        .build(),
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("appnexus")
                                                        .status("success")
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .bidResponse(org.prebid.server.rubicon.analytics.proto
                                                                .BidResponse.of(567, BigDecimal.valueOf(6.78),
                                                                        "video", Dimensions.of(600, 700)))
                                                        .build(),
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("rubicon")
                                                        .status("no-bid")
                                                        .source("server")
                                                        .serverLatencyMillis(101)
                                                        .serverHasUserId(true)
                                                        .params(Params.of(null, null, null))
                                                        .build(),
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("pubmatic")
                                                        .status("error")
                                                        .error(BidError.timeoutError("Timeout error"))
                                                        .source("server")
                                                        .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId3")
                                        .status("no-bid")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(400, 500)))
                                        .siteId(654)
                                        .zoneId(987)
                                        .adUnitCode("storedId1")
                                        .pbAdSlot("pbAdSlot1")
                                        .gam(Gam.of("adSlot1"))
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("rubicon")
                                                        .status("no-bid")
                                                        .source("server")
                                                        .serverLatencyMillis(101)
                                                        .serverHasUserId(true)
                                                        .params(Params.of(321, 654, 987))
                                                        .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId4")
                                        .status("no-bid")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(500, 600)))
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("appnexus")
                                                        .status("no-bid")
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId5")
                                        .status("no-bid")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(600, 700)))
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("unknown")
                                                        .status("no-bid")
                                                        .source("server")
                                                        .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId6")
                                        .status("error")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(800, 900)))
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("appnexus")
                                                        .status("error")
                                                        .error(BidError.timeoutError("Timeout error"))
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .build()))
                                        .error(Error.of("timeout-error", "Timeout error"))
                                        .build()),
                        123,
                        1000L,
                        true,
                        Gdpr.of(true, false, null, null))))
                .build());
    }

    @Test
    public void processNotificationEventShouldPostEventToEndpointWithWinExpectedBody() throws IOException {
        // given
        givenHttpClientReturnsResponse(200, null);

        final Map<String, String> headers = new HashMap<>();
        headers.put("Referer", "http://referer");
        headers.put("User-Agent", "ua");
        headers.put("DNT", "1");
        final HttpContext httpContext = HttpContext.builder().headers(headers).cookies(emptyMap()).build();
        final NotificationEvent event = NotificationEvent.builder()
                .type(NotificationEvent.Type.win)
                .bidId("bidid")
                .account(Account.builder().id("123123").build())
                .timestamp(1000L)
                .integration("integration")
                .httpContext(httpContext)
                .lineItemId("lineItemId")
                .build();

        // when
        reporter.processEvent(event);

        // then
        final ArgumentCaptor<String> eventCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(eq("http://host-url/event"), any(), eventCaptor.capture(), anyLong());

        final Event expectedWinEvent = Event.builder()
                .integration("integration")
                .version("pbs-version-1")
                .referrerUri("http://referer")
                .referrerHostname("referer")
                .limitAdTracking(true)
                .userAgent("ua")
                .bidsWon(singletonList(BidWon.builder()
                        .accountId(123123)
                        .bidId("bidid")
                        .status("success")
                        .source("server")
                        .serverHasUserId(true)
                        .hasRubiconId(true)
                        .build()))
                .eventCreator(EventCreator.of("pbsHostname", "dataCenterRegion"))
                .build();

        final Event actual = mapper.readValue(eventCaptor.getValue(), Event.class);
        then(actual).isEqualToIgnoringGivenFields(expectedWinEvent, "eventTimeMillis");
        assertThat(actual.getEventTimeMillis()).isEqualTo(1000L);
    }

    @Test
    public void processNotificationEventShouldPostEventToEndpointWithImpExpectedBody() throws IOException {
        // given
        givenHttpClientReturnsResponse(200, null);

        final Map<String, String> headers = new HashMap<>();
        headers.put("Referer", "http://referer");
        headers.put("User-Agent", "ua");
        headers.put("DNT", "1");
        final HttpContext httpContext = HttpContext.builder().headers(headers).cookies(emptyMap()).build();
        final NotificationEvent event = NotificationEvent.builder()
                .type(NotificationEvent.Type.imp)
                .bidId("bidid")
                .bidder("bidder")
                .account(Account.builder().id("222").build())
                .httpContext(httpContext)
                .lineItemId("lineItemId")
                .build();

        // when
        reporter.processEvent(event);

        // then
        final ArgumentCaptor<String> eventCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(eq("http://host-url/event"), any(), eventCaptor.capture(), anyLong());

        final Event expectedWinEvent = Event.builder()
                .integration("pbs")
                .version("pbs-version-1")
                .referrerUri("http://referer")
                .referrerHostname("referer")
                .limitAdTracking(true)
                .userAgent("ua")
                .impressions(singletonList(Impression.builder()
                        .bidId("bidid")
                        .bidder("bidder")
                        .accountId(222)
                        .status("success")
                        .source("server")
                        .serverHasUserId(true)
                        .hasRubiconId(true)
                        .build()))
                .eventCreator(EventCreator.of("pbsHostname", "dataCenterRegion"))
                .build();

        final Event actual = mapper.readValue(eventCaptor.getValue(), Event.class);
        then(actual).isEqualToIgnoringGivenFields(expectedWinEvent, "eventTimeMillis");
        assertThat(actual.getEventTimeMillis()).isCloseTo(Instant.now().toEpochMilli(), within(400L));
    }

    @Test
    public void processNotificationEventShouldUseAccountSamplingFactor() {
        // given
        givenHttpClientReturnsResponse(200, null);

        final HttpContext httpContext = HttpContext.builder().headers(emptyMap()).cookies(emptyMap()).build();
        final NotificationEvent event = NotificationEvent.builder()
                .type(NotificationEvent.Type.win)
                .bidId("bidid")
                .account(Account.builder()
                        .id("123123")
                        .analytics(AccountAnalyticsConfig.of(
                                null,
                                singletonMap("rubicon",
                                        mapper.createObjectNode()
                                                .put("sampling-factor", 10))))
                        .build())
                .httpContext(httpContext)
                .build();

        // when
        for (int i = 0; i < 10; i++) {
            reporter.processEvent(event);
        }

        // then
        verify(httpClient).post(anyString(), any(), any(), anyLong());
    }

    @Test
    public void processNotificationEventShouldNotSendEventIfAccountSamplingFactorIsMissing() {
        // given
        final HttpContext httpContext = HttpContext.builder().headers(emptyMap()).cookies(emptyMap()).build();
        final NotificationEvent event = NotificationEvent.builder()
                .type(NotificationEvent.Type.win)
                .bidId("bidid")
                .account(Account.builder().id("123123").build())
                .httpContext(httpContext)
                .build();

        reporter = new RubiconAnalyticsReporter(HOST_URL, null, null, "pbsHostname", PBS_HOST_VENDOR_ID,
                "dataCenterRegion", bidderCatalog, uidsCookieService, uidsAuditCookieService, currencyService,
                ipAddressHelper, httpClient, false, jacksonMapper);

        // when
        reporter.processEvent(event);

        // then
        verifyZeroInteractions(httpClient);
    }

    @Test
    public void processAmpEventShouldIgnoreProcessingIfAuctionContextIsMissing() {
        // given
        final AmpEvent ampEvent = AmpEvent.builder()
                .auctionContext(null)
                .build();

        // when
        reporter.processEvent(ampEvent);

        // then
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAmpEventShouldIgnoreProcessingIfHttpContextIsMissing() {
        // given
        final AmpEvent ampEvent = AmpEvent.builder()
                .auctionContext(AuctionContext.builder().build())
                .httpContext(null)
                .build();

        // when
        reporter.processEvent(ampEvent);

        // then
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAmpEventShouldIgnoreProcessingIfBidRequestIsMissing() {
        // given
        final AmpEvent ampEvent = AmpEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(null)
                        .build())
                .httpContext(HttpContext.builder().build())
                .build();

        // when
        reporter.processEvent(ampEvent);

        // then
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAmpEventShouldIgnoreProcessingIfAccountIsMissing() {
        // given
        final AmpEvent ampEvent = AmpEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder().build())
                        .account(null)
                        .build())
                .httpContext(HttpContext.builder().build())
                .build();

        // when
        reporter.processEvent(ampEvent);

        // then
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAmpEventShouldIgnoreProcessingIfBidResponseIsMissing() {
        // given
        final AmpEvent ampEvent = AmpEvent.builder()
                .auctionContext(AuctionContext.builder()
                        .bidRequest(BidRequest.builder().build())
                        .account(Account.builder().build())
                        .build())
                .httpContext(HttpContext.builder().build())
                .bidResponse(null)
                .build();

        // when
        reporter.processEvent(ampEvent);

        // then
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @Test
    public void processAmpEventShouldIgnoreNonWebRequests() {
        // given
        final AmpEvent ampEvent = AmpEvent.builder()
                .auctionContext(givenAuctionContext(
                        BidRequest.builder()
                                .app(App.builder().build())
                                .build()))
                .build();

        // when
        reporter.processEvent(ampEvent);

        // then
        verifyZeroInteractions(bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient);
    }

    @SuppressWarnings("checkstyle:methodlength")
    @Test
    public void processAmpEventShouldPostEventToEndpointWithExpectedBody() throws IOException {
        // given
        givenHttpClientReturnsResponse(200, null);
        given(bidderCatalog.isValidName("unknown")).willReturn(false);
        givenCurrencyConversion(BigDecimal.TEN);
        httpContext = HttpContext.builder().uri("http://host-url/event/tag_id=storedId1").cookies(emptyMap()).build();
        final AmpEvent event = AmpEvent.builder()
                .httpContext(httpContext)
                .auctionContext(givenAuctionContext(sampleAmpBidRequest()))
                .bidResponse(sampleBidResponse())
                .build();

        // when
        reporter.processEvent(event);

        // then
        final ArgumentCaptor<String> eventCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(eq("http://host-url/event"), any(), eventCaptor.capture(), anyLong());

        then(mapper.readValue(eventCaptor.getValue(), Event.class)).isEqualTo(expectedEventBuilderBaseFromSite()
                .auctions(singletonList(Auction.of("bidRequestId", 1,
                        asList(
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId1")
                                        .status("success")
                                        .mediaTypes(asList("banner", "video"))
                                        .videoAdFormat("interstitial")
                                        .dimensions(asList(Dimensions.of(200, 300), Dimensions.of(300, 400)))
                                        .adserverTargeting(singletonMap("key1", "value1"))
                                        .siteId(456)
                                        .zoneId(789)
                                        .adUnitCode("storedId1")
                                        .pbAdSlot("pbAdSlot1")
                                        .bids(singletonList(org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                .bidId("bidId1")
                                                .bidder("rubicon")
                                                .status("success")
                                                .source("server")
                                                .serverLatencyMillis(101)
                                                .serverHasUserId(true)
                                                .params(Params.of(123, 456, 789))
                                                .bidResponse(
                                                        org.prebid.server.rubicon.analytics.proto
                                                                .BidResponse.of(345, BigDecimal.valueOf(0.46),
                                                                        "video", Dimensions.of(500, 600)))
                                                .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId2")
                                        .status("success")
                                        .mediaTypes(singletonList("video"))
                                        .videoAdFormat("mid-roll")
                                        .dimensions(singletonList(Dimensions.of(100, 200)))
                                        .adserverTargeting(singletonMap("key22", "value22"))
                                        .adUnitCode("storedId1")
                                        .pbAdSlot("pbAdSlot1")
                                        .bids(asList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidId("bidId2")
                                                        .bidder("appnexus")
                                                        .status("success")
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .bidResponse(
                                                                org.prebid.server.rubicon.analytics.proto
                                                                        .BidResponse.of(456, BigDecimal.valueOf(0.57),
                                                                                "video", Dimensions.of(600, 700)))
                                                        .build(),
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("appnexus")
                                                        .status("success")
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .bidResponse(
                                                                org.prebid.server.rubicon.analytics.proto
                                                                        .BidResponse.of(567, BigDecimal.valueOf(0.68),
                                                                                "video", Dimensions.of(600, 700)))
                                                        .build(),
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("rubicon")
                                                        .status("no-bid")
                                                        .source("server")
                                                        .serverLatencyMillis(101)
                                                        .serverHasUserId(true)
                                                        .params(Params.of(null, null, null))
                                                        .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId3")
                                        .status("no-bid")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(400, 500)))
                                        .siteId(654)
                                        .zoneId(987)
                                        .adUnitCode("storedId1")
                                        .pbAdSlot("pbAdSlot1")
                                        .bids(singletonList(org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                .bidder("rubicon")
                                                .status("no-bid")
                                                .source("server")
                                                .serverLatencyMillis(101)
                                                .serverHasUserId(true)
                                                .params(Params.of(321, 654, 987))
                                                .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId4")
                                        .status("no-bid")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(500, 600)))
                                        .adUnitCode("storedId1")
                                        .bids(singletonList(org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                .bidder("appnexus")
                                                .status("no-bid")
                                                .source("server")
                                                .serverLatencyMillis(202)
                                                .serverHasUserId(false)
                                                .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId5")
                                        .status("no-bid")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(600, 700)))
                                        .adUnitCode("storedId1")
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("unknown")
                                                        .status("no-bid")
                                                        .source("server")
                                                        .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("bidRequestId-impId6")
                                        .status("error")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(800, 900)))
                                        .adUnitCode("storedId1")
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("appnexus")
                                                        .status("error")
                                                        .error(BidError.timeoutError("Timeout error"))
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .build()))
                                        .error(Error.of("timeout-error", "Timeout error"))
                                        .build()),
                        123,
                        1000L,
                        true,
                        Gdpr.of(true, false, null, null))))
                .build());
    }

    private static BidRequest sampleAuctionBidRequest(String integration, String wrappername) {
        final ObjectNode multiBidderImpExt = mapper.createObjectNode()
                .<ObjectNode>set("prebid", mapper.createObjectNode()
                        .set("bidder", mapper.createObjectNode()
                                .<ObjectNode>set("appnexus", mapper.createObjectNode())
                                .<ObjectNode>set("rubicon", mapper.createObjectNode())
                                .<ObjectNode>set("pubmatic", mapper.createObjectNode())))
                .set("context", mapper.createObjectNode()); // should be ignored

        final ObjectNode rubiconExtWithStoredId = mapper.createObjectNode()
                .<ObjectNode>set("prebid", mapper.createObjectNode()
                        .<ObjectNode>set("bidder", mapper.createObjectNode()
                                .set("rubicon", mapper.valueToTree(
                                        ExtImpRubicon.builder()
                                                .video(RubiconVideoParams.builder().sizeId(202).build())
                                                .accountId(321)
                                                .siteId(654)
                                                .zoneId(987)
                                                .build())))
                        .set("storedrequest", mapper.valueToTree(ExtStoredRequest.of("storedId1"))))
                .set("context", mapper.valueToTree(ExtImpContext.of(
                        mapper.createObjectNode()
                                .put("pbadslot", "pbAdSlot1")
                                .set("adserver", mapper.createObjectNode()
                                        .put("name", "gam")
                                        .put("adSlot", "adSlot1")))));
        return BidRequest.builder()
                .id("bidRequestId")
                .site(Site.builder()
                        .page("refererPage")
                        .build())
                .device(Device.builder()
                        .connectiontype(17)
                        .lmt(1)
                        .ua("userAgent")
                        .ip("104.22.41.73")
                        .ipv6("2606:4700:10::6816:2849")
                        .geo(Geo.builder().country("countryFromRequest").build())
                        .build())
                .app(App.builder()
                        .bundle("bundle")
                        .ver("version")
                        .ext(ExtApp.of(ExtAppPrebid.of("sdkSource", "sdkVersion"), null))
                        .publisher(Publisher.builder().id("1234").build())
                        .build())
                .imp(asList(
                        Imp.builder().id("impId1")
                                .banner(Banner.builder()
                                        .format(asList(
                                                Format.builder().w(200).h(300).build(),
                                                Format.builder().w(300).h(400).build()))
                                        .build())
                                .video(Video.builder().build())
                                .ext(mapper.createObjectNode()
                                        .set("prebid", mapper.createObjectNode()
                                                .set("bidder", mapper.createObjectNode()
                                                        .set("rubicon", mapper.valueToTree(
                                                                ExtImpRubicon.builder()
                                                                        .video(RubiconVideoParams.builder()
                                                                                .sizeId(202)
                                                                                .build())
                                                                        .accountId(123)
                                                                        .siteId(456)
                                                                        .zoneId(789)
                                                                        .build())))))
                                .build(),
                        Imp.builder().id("impId2")
                                .video(Video.builder().startdelay(-1).w(100).h(200).build())
                                .ext(multiBidderImpExt)
                                .build(),
                        Imp.builder().id("impId3")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(400).h(500).build()))
                                        .build())
                                .ext(rubiconExtWithStoredId)
                                .build(),
                        Imp.builder().id("impId4")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(500).h(600).build()))
                                        .build())
                                .ext(mapper.createObjectNode()
                                        .set("prebid", mapper.createObjectNode()
                                                .set("bidder", mapper.createObjectNode()
                                                        .set("appnexus", mapper.createObjectNode()))))
                                .build(),
                        Imp.builder().id("impId5")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(600).h(700).build()))
                                        .build())
                                .ext(mapper.createObjectNode()
                                        .set("prebid", mapper.createObjectNode()
                                                .set("bidder", mapper.createObjectNode()
                                                        .set("unknown", mapper.createObjectNode()))))
                                .build(),
                        Imp.builder().id("impId6")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(800).h(900).build()))
                                        .build())
                                .ext(mapper.createObjectNode()
                                        .set("prebid", mapper.createObjectNode()
                                                .set("bidder", mapper.createObjectNode()
                                                        .set("appnexus", mapper.createObjectNode()))))
                                .build()))
                .cur(singletonList("USD"))
                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .integration(integration)
                        .bidders(mapper.valueToTree(ExtRequestPrebidBidders.of(
                                ExtRequestPrebidBiddersRubicon.of(null, wrappername))))
                        .channel(ExtRequestPrebidChannel.of("app"))
                        .build()))
                .tmax(1000L)
                .build();
    }

    private static BidRequest sampleAmpBidRequest() {
        final ObjectNode impContextDataNode = mapper.createObjectNode().put("pbadslot", "pbAdSlot1");

        final ObjectNode multiBidderImpExt = mapper.createObjectNode()
                .<ObjectNode>set("prebid", mapper.createObjectNode()
                        .set("bidder", mapper.createObjectNode()
                                .<ObjectNode>set("appnexus", mapper.createObjectNode())
                                .set("rubicon", mapper.createObjectNode())))
                .set("context", mapper.valueToTree(ExtImpContext.of(impContextDataNode)));

        final ObjectNode rubiconExtWithStoredId = mapper.createObjectNode()
                .<ObjectNode>set("prebid", mapper.createObjectNode()
                        .<ObjectNode>set("bidder", mapper.createObjectNode()
                                .set("rubicon", mapper.valueToTree(
                                        ExtImpRubicon.builder()
                                                .video(RubiconVideoParams.builder().sizeId(202).build())
                                                .accountId(321)
                                                .siteId(654)
                                                .zoneId(987)
                                                .build())))
                        .set("storedrequest", mapper.valueToTree(ExtStoredRequest.of("storedId1"))))
                .set("context", mapper.valueToTree(ExtImpContext.of(impContextDataNode)));

        return BidRequest.builder()
                .id("bidRequestId")
                .device(Device.builder()
                        .lmt(1)
                        .ua("userAgent")
                        .build())
                .site(Site.builder()
                        .publisher(Publisher.builder().id("1234").build())
                        .page("http://referer/page")
                        .build())
                .imp(asList(
                        Imp.builder().id("impId1")
                                .banner(Banner.builder()
                                        .format(asList(
                                                Format.builder().w(200).h(300).build(),
                                                Format.builder().w(300).h(400).build()))
                                        .build())
                                .video(Video.builder().build())
                                .ext(mapper.createObjectNode()
                                        .<ObjectNode>set("prebid", mapper.createObjectNode()
                                                .<ObjectNode>set("bidder", mapper.createObjectNode()
                                                        .<ObjectNode>set("rubicon", mapper.valueToTree(
                                                                ExtImpRubicon.builder()
                                                                        .video(RubiconVideoParams.builder()
                                                                                .sizeId(202)
                                                                                .build())
                                                                        .accountId(123)
                                                                        .siteId(456)
                                                                        .zoneId(789)
                                                                        .build()))))
                                        .set("context", mapper.valueToTree(ExtImpContext.of(impContextDataNode))))
                                .build(),
                        Imp.builder().id("impId2")
                                .video(Video.builder().startdelay(-1).w(100).h(200).build())
                                .ext(multiBidderImpExt)
                                .build(),
                        Imp.builder().id("impId3")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(400).h(500).build()))
                                        .build())
                                .ext(rubiconExtWithStoredId)
                                .build(),
                        Imp.builder().id("impId4")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(500).h(600).build()))
                                        .build())
                                .ext(mapper.createObjectNode()
                                        .set("prebid", mapper.createObjectNode()
                                                .<ObjectNode>set("bidder", mapper.createObjectNode()
                                                        .set("appnexus", mapper.createObjectNode()))))
                                .build(),
                        Imp.builder().id("impId5")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(600).h(700).build()))
                                        .build())
                                .ext(mapper.createObjectNode()
                                        .set("prebid", mapper.createObjectNode()
                                                .<ObjectNode>set("bidder", mapper.createObjectNode()
                                                        .set("unknown", mapper.createObjectNode()))))
                                .build(),
                        Imp.builder().id("impId6")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(800).h(900).build()))
                                        .build())
                                .ext(mapper.createObjectNode()
                                        .set("prebid", mapper.createObjectNode()
                                                .<ObjectNode>set("bidder", mapper.createObjectNode()
                                                        .set("appnexus", mapper.createObjectNode()))))
                                .build()))
                .ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .channel(ExtRequestPrebidChannel.of("amp"))
                        .build()))
                .cur(singletonList("JPY"))
                .tmax(1000L)
                .build();
    }

    private static BidResponse sampleBidResponse() {
        final Map<String, List<ExtBidderError>> errors = doubleMap(
                "appnexus", singletonList(ExtBidderError.of(1, "Timeout error", singleton("impId6"))),
                "pubmatic", singletonList(ExtBidderError.of(1, "Timeout error", singleton("impId2"))));

        return BidResponse.builder()
                .seatbid(asList(
                        SeatBid.builder()
                                .seat("rubicon")
                                .bid(singletonList(Bid.builder()
                                        .id("bidId1")
                                        .impid("impId1")
                                        .dealid("345")
                                        .price(BigDecimal.valueOf(4.56))
                                        .w(500)
                                        .h(600)
                                        .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.builder()
                                                .bidid("bidId1")
                                                .type(BidType.video)
                                                .targeting(singletonMap("key1", "value1"))
                                                .build(), null)))
                                        .build()))
                                .build(),
                        SeatBid.builder()
                                .seat("appnexus")
                                .bid(asList(
                                        Bid.builder()
                                                .id("bidId2")
                                                .impid("impId2")
                                                .dealid("456")
                                                .price(BigDecimal.valueOf(5.67))
                                                .w(600)
                                                .h(700)
                                                .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.builder()
                                                                .bidid("bidId2")
                                                                .type(BidType.video)
                                                                .targeting(singletonMap("key21", "value21"))
                                                                .build(),
                                                        null)))
                                                .build(),
                                        Bid.builder()
                                                .id(null) // mark as optional
                                                .impid("impId2")
                                                .dealid("567")
                                                .price(BigDecimal.valueOf(6.78))
                                                .w(600)
                                                .h(700)
                                                .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.builder()
                                                                .bidid(null)
                                                                .type(BidType.video)
                                                                .targeting(singletonMap("key22", "value22"))
                                                                .build(),
                                                        null)))
                                                .build()))
                                .build()))
                .ext(ExtBidResponse.builder()
                        .errors(errors)
                        .responsetimemillis(doubleMap("rubicon", 101, "appnexus", 202))
                        .build())
                .build();
    }

    @SuppressWarnings("SameParameterValue")
    private static <K, V> Map<K, V> doubleMap(K key1, V value1, K key2, V value2) {
        final Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private static Event.EventBuilder expectedEventBuilderBaseFromApp() {
        return Event.builder()
                .integration("dbpg")
                .wrapperName("12314wp")
                .version("pbs-version-1")
                .client(Client.builder()
                        .deviceClass("APP")
                        .app(org.prebid.server.rubicon.analytics.proto.App.of("bundle", "version", "sdkVersion",
                                "sdkSource"))
                        .connectionType(17)
                        .build())
                .limitAdTracking(true)
                .eventCreator(EventCreator.of("pbsHostname", "dataCenterRegion"))
                .userAgent("userAgent")
                .referrerUri("refererPage")
                .channel("app")
                .user(User.of(org.prebid.server.rubicon.analytics.proto.Geo.of("countryFromRequest", 123)));
    }

    private static Event.EventBuilder expectedEventBuilderBaseFromSite() {
        return Event.builder()
                .integration("pbs")
                .version("pbs-version-1")
                .limitAdTracking(true)
                .eventCreator(EventCreator.of("pbsHostname", "dataCenterRegion"))
                .userAgent("userAgent")
                .channel("amp")
                .user(User.of(org.prebid.server.rubicon.analytics.proto.Geo.of("countryFromAuditCookie", 123)))
                .referrerUri("http://referer/page")
                .referrerHostname("referer");
    }

    @SuppressWarnings("SameParameterValue")
    private void givenHttpClientReturnsResponse(int statusCode, String response) {
        final HttpClientResponse httpClientResponse = HttpClientResponse.of(statusCode, null, response);
        given(httpClient.post(anyString(), any(), any(), anyLong()))
                .willReturn(Future.succeededFuture(httpClientResponse));
    }

    private void givenHttpClientProducesException(Throwable throwable) {
        given(httpClient.post(anyString(), any(), any(), anyLong()))
                .willReturn(Future.failedFuture(throwable));
    }

    private AuctionContext givenAuctionContext(BidRequest bidRequest, Integer samplingFactor) {
        return AuctionContext.builder()
                .bidRequest(bidRequest)
                .account(Account.builder()
                        .id("123")
                        .analytics(AccountAnalyticsConfig.of(
                                null,
                                singletonMap("rubicon",
                                        mapper.createObjectNode()
                                                .put("sampling-factor", samplingFactor))))
                        .build())
                .privacyContext(PrivacyContext.of(null, TcfContext.builder().gdpr("1").build()))
                .geoInfo(GeoInfo.builder().vendor("vendor").metroNielsen(123).build())
                .build();
    }

    private AuctionContext givenAuctionContext(BidRequest bidRequest) {
        return givenAuctionContext(bidRequest, null);
    }

    @SuppressWarnings("SameParameterValue")
    private void givenCurrencyConversion(BigDecimal value) {
        given(currencyService.convertCurrency(any(), any(), any(), any(), any()))
                .willAnswer(invocationOnMock ->
                        invocationOnMock.getArgument(0) != null
                                ? new BigDecimal(invocationOnMock.getArgument(0).toString())
                                .divide(value, RoundingMode.HALF_DOWN) : null);
    }
}
