package org.prebid.server.rubicon.analytics;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Device;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
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
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.bidder.Usersyncer;
import org.prebid.server.cookie.UidsCookie;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.rubicon.ExtImpRubicon;
import org.prebid.server.proto.openrtb.ext.request.rubicon.RubiconVideoParams;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponse;
import org.prebid.server.rubicon.analytics.proto.AdUnit;
import org.prebid.server.rubicon.analytics.proto.Auction;
import org.prebid.server.rubicon.analytics.proto.BidWon;
import org.prebid.server.rubicon.analytics.proto.Client;
import org.prebid.server.rubicon.analytics.proto.Dimensions;
import org.prebid.server.rubicon.analytics.proto.Event;
import org.prebid.server.rubicon.analytics.proto.EventCreator;
import org.prebid.server.rubicon.analytics.proto.ExtApp;
import org.prebid.server.rubicon.analytics.proto.ExtAppPrebid;
import org.prebid.server.rubicon.analytics.proto.Params;
import org.prebid.server.vertx.http.HttpClient;
import org.prebid.server.vertx.http.model.HttpClientResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class RubiconAnalyticsModuleTest extends VertxTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private BidderCatalog bidderCatalog;
    @Mock
    private UidsCookieService uidsCookieService;
    @Mock
    private Usersyncer rubiconUsersyncer;
    @Mock
    private Usersyncer appnexusUsersyncer;
    @Mock
    private UidsCookie uidsCookie;
    @Mock
    private HttpClient httpClient;

    private RubiconAnalyticsModule module;

    @Before
    public void setUp() {
        given(bidderCatalog.isValidName("rubicon")).willReturn(true);
        given(bidderCatalog.isValidName("appnexus")).willReturn(true);
        given(bidderCatalog.usersyncerByName("rubicon")).willReturn(rubiconUsersyncer);
        given(bidderCatalog.usersyncerByName("appnexus")).willReturn(appnexusUsersyncer);

        given(rubiconUsersyncer.cookieFamilyName()).willReturn("rubicon");
        given(appnexusUsersyncer.cookieFamilyName()).willReturn("appnexus");

        given(uidsCookie.hasLiveUidFrom("rubicon")).willReturn(true);
        given(uidsCookie.hasLiveUidFrom("appnexus")).willReturn(false);

        given(uidsCookieService.parseHostCookie(any())).willReturn("khaos-cookie-value");

        module = new RubiconAnalyticsModule("url", 1, "pbs-version-1", "pbsHostname", "dataCenterRegion", bidderCatalog,
                uidsCookieService, httpClient);
    }

    @Test
    public void processEventShouldTakeIntoAccountSamplingFactor() {
        // given
        module = new RubiconAnalyticsModule("url", 10, "pbs-version-1", "pbsHostname", "dataCenterRegion",
                bidderCatalog, uidsCookieService, httpClient);

        givenHttpClientReturnsResponse(200, null);

        // when
        for (int i = 0; i < 10; i++) {
            module.processEvent(AuctionEvent.builder()
                    .bidRequest(BidRequest.builder()
                            .imp(emptyList())
                            .app(App.builder().build())
                            .build())
                    .uidsCookie(uidsCookie)
                    .bidResponse(BidResponse.builder()
                            .seatbid(emptyList())
                            .build())
                    .build());
        }

        // then
        verify(httpClient).post(anyString(), any(), any(), anyLong());
    }

    @Test
    public void processEventShouldIgnoreNonAuctionEvents() {
        // when
        module.processEvent(AmpEvent.builder().build());

        // then
        verifyZeroInteractions(httpClient);
    }

    @Test
    public void processEventShouldIgnoreNonMobileRequests() {
        // when
        module.processEvent(AuctionEvent.builder()
                .bidRequest(BidRequest.builder()
                        .site(Site.builder().build())
                        .build())
                .build());

        // then
        verifyZeroInteractions(httpClient);
    }

    @Test
    public void processEventShouldPostEventToEndpointWithExpectedHeaders() {
        // given
        givenHttpClientReturnsResponse(200, null);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .bidRequest(sampleBidRequest())
                .uidsCookie(uidsCookie)
                .bidResponse(sampleBidResponse())
                .build();

        // when
        module.processEvent(auctionEvent);

        // then
        final ArgumentCaptor<MultiMap> captor = ArgumentCaptor.forClass(MultiMap.class);
        verify(httpClient).post(anyString(), captor.capture(), any(), anyLong());

        assertThat(captor.getValue().entries())
                .extracting(Map.Entry::getKey, Map.Entry::getValue)
                .containsOnly(tuple(HttpHeaders.USER_AGENT.toString(), "userAgent"));
    }

    @Test
    public void processEventShouldPostEventToEndpointWithExpectedBody() throws IOException {
        // given
        givenHttpClientReturnsResponse(200, null);
        given(bidderCatalog.isValidName("unknown")).willReturn(false);

        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .bidRequest(sampleBidRequest())
                .uidsCookie(uidsCookie)
                .bidResponse(sampleBidResponse())
                .build();

        // when
        module.processEvent(auctionEvent);

        // then
        final ArgumentCaptor<String> eventCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(eq("url/event"), any(), eventCaptor.capture(), anyLong());

        then(mapper.readValue(eventCaptor.getValue(), Event.class)).isEqualTo(expectedEventBuilderBase()
                .auctions(singletonList(Auction.of(
                        "bidRequestId",
                        1,
                        asList(
                                AdUnit.builder()
                                        .transactionId("impId1")
                                        .status("success")
                                        .mediaTypes(asList("banner", "video"))
                                        .videoAdFormat("interstitial")
                                        .dimensions(asList(Dimensions.of(200, 300), Dimensions.of(300, 400)))
                                        .adServerTargeting(singletonMap("key1", "value1"))
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("rubicon")
                                                        .status("success")
                                                        .source("server")
                                                        .serverLatencyMillis(101)
                                                        .serverHasUserId(true)
                                                        .params(Params.of(123, 456, 789))
                                                        .bidResponse(
                                                                org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                                        345, BigDecimal.valueOf(4.56), "video",
                                                                        Dimensions.of(500, 600)))
                                                        .build()))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("impId2")
                                        .status("success")
                                        .mediaTypes(singletonList("video"))
                                        .videoAdFormat("mid-roll")
                                        .dimensions(singletonList(Dimensions.of(100, 200)))
                                        .adServerTargeting(singletonMap("key22", "value22"))
                                        .bids(asList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("appnexus")
                                                        .status("success")
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .bidResponse(
                                                                org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                                        456, BigDecimal.valueOf(5.67), "video",
                                                                        Dimensions.of(600, 700)))
                                                        .build(),
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("appnexus")
                                                        .status("success")
                                                        .source("server")
                                                        .serverLatencyMillis(202)
                                                        .serverHasUserId(false)
                                                        .bidResponse(
                                                                org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                                        567, BigDecimal.valueOf(6.78), "video",
                                                                        Dimensions.of(600, 700)))
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
                                        .transactionId("impId3")
                                        .status("no-bid")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(400, 500)))
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
                                        .transactionId("impId4")
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
                                        .transactionId("impId5")
                                        .status("no-bid")
                                        .mediaTypes(singletonList("banner"))
                                        .dimensions(singletonList(Dimensions.of(600, 700)))
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                                                        .bidder("unknown")
                                                        .status("no-bid")
                                                        .source("server")
                                                        .build()))
                                        .build()),
                        1234, 1000L, true)))
                .build());
    }

    @Test
    public void postProcessShouldTakeIntoAccountSamplingFactor() {
        // given
        module = new RubiconAnalyticsModule("url", 2, "pbs-version-1", "pbsHostname", "dataCenterRegion", bidderCatalog,
                uidsCookieService, httpClient);

        final Bid bid1 = Bid.builder().build();
        final Bid bid2 = Bid.builder().build();

        // when
        module.postProcess(
                null,
                BidRequest.builder()
                        .imp(emptyList())
                        .app(App.builder().build())
                        .build(),
                uidsCookie,
                BidResponse.builder()
                        .seatbid(singletonList(SeatBid.builder().bid(asList(bid1, bid2)).build()))
                        .build());

        // then
        then(bid1.getNurl()).isNull();
        then(bid2.getNurl()).isNotNull();
    }

    @Test
    public void postProcessShouldIgnoreNonMobileRequests() {
        // given
        final Bid bid = Bid.builder().build();

        // when
        module.postProcess(
                null,
                BidRequest.builder()
                        .imp(emptyList())
                        .build(),
                null,
                BidResponse.builder()
                        .seatbid(singletonList(SeatBid.builder().bid(singletonList(bid)).build()))
                        .build());

        // then
        then(bid.getNurl()).isNull();
    }

    @Test
    public void postProcessShouldSetBidNurlWithEventData() {
        // given
        final BidRequest bidRequest = sampleBidRequest();
        final BidResponse bidResponse = sampleBidResponse();

        // when
        final BidResponse returnedBidResponse = module.postProcess(null, bidRequest, uidsCookie, bidResponse).result();

        // then
        then(returnedBidResponse.getSeatbid())
                .flatExtracting(SeatBid::getBid)
                .extracting(Bid::getNurl)
                .allMatch(nurl -> nurl.startsWith("url/event?type=bidWon&data="))
                .extracting(nurl -> nurl.replaceFirst("url/event\\?type=bidWon&data=", ""))
                .extracting(payload -> mapper.readValue(Base64.getUrlDecoder().decode(payload), Event.class))
                .containsOnly(expectedEventBuilderBase()
                                .bidsWon(singletonList(BidWon.builder()
                                        .transactionId("impId1")
                                        .accountId(1234)
                                        .bidder("rubicon")
                                        .samplingFactor(1)
                                        .bidwonStatus("success")
                                        .mediaTypes(asList("banner", "video-instream"))
                                        .videoAdFormat("interstitial")
                                        .source("server")
                                        .bidResponse(org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                345, BigDecimal.valueOf(4.56), "video", Dimensions.of(500, 600)))
                                        .serverLatencyMillis(101)
                                        .serverHasUserId(true)
                                        .hasRubiconId(true)
                                        .params(Params.of(123, 456, 789))
                                        .build()))
                                .build(),
                        expectedEventBuilderBase()
                                .bidsWon(singletonList(BidWon.builder()
                                        .transactionId("impId2")
                                        .accountId(1234)
                                        .bidder("appnexus")
                                        .samplingFactor(1)
                                        .bidwonStatus("success")
                                        .mediaTypes(singletonList("video-instream"))
                                        .videoAdFormat("mid-roll")
                                        .source("server")
                                        .bidResponse(org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                456, BigDecimal.valueOf(5.67), "video", Dimensions.of(600, 700)))
                                        .serverLatencyMillis(202)
                                        .serverHasUserId(false)
                                        .hasRubiconId(true)
                                        .build()))
                                .build(),
                        expectedEventBuilderBase()
                                .bidsWon(singletonList(BidWon.builder()
                                        .transactionId("impId2")
                                        .accountId(1234)
                                        .bidder("appnexus")
                                        .samplingFactor(1)
                                        .bidwonStatus("success")
                                        .mediaTypes(singletonList("video-instream"))
                                        .videoAdFormat("mid-roll")
                                        .source("server")
                                        .bidResponse(org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                567, BigDecimal.valueOf(6.78), "video", Dimensions.of(600, 700)))
                                        .serverLatencyMillis(202)
                                        .serverHasUserId(false)
                                        .hasRubiconId(true)
                                        .build()))
                                .build());
    }

    private static BidRequest sampleBidRequest() {
        final ObjectNode multiBidderImpExt = mapper.createObjectNode();
        multiBidderImpExt.set("appnexus", mapper.createObjectNode());
        multiBidderImpExt.set("rubicon", mapper.createObjectNode());

        return BidRequest.builder()
                .id("bidRequestId")
                .device(Device.builder()
                        .os("os")
                        .osv("2.23")
                        .make("osMake")
                        .model("osModel")
                        .carrier("carrier")
                        .connectiontype(17)
                        .lmt(1)
                        .ua("userAgent")
                        .build())
                .app(App.builder()
                        .bundle("bundle")
                        .ver("version")
                        .ext(mapper.valueToTree(ExtApp.of(ExtAppPrebid.of("sdkVersion", "sdkSource"))))
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
                                .ext((ObjectNode) mapper.createObjectNode().set("rubicon", mapper.valueToTree(
                                        ExtImpRubicon.builder()
                                                .video(RubiconVideoParams.builder().sizeId(202).build())
                                                .accountId(123)
                                                .siteId(456)
                                                .zoneId(789)
                                                .build())))
                                .build(),
                        Imp.builder().id("impId2")
                                .video(Video.builder().startdelay(-1).w(100).h(200).build())
                                .ext(multiBidderImpExt)
                                .build(),
                        Imp.builder().id("impId3")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(400).h(500).build()))
                                        .build())
                                .ext((ObjectNode) mapper.createObjectNode().set("rubicon", mapper.valueToTree(
                                        ExtImpRubicon.builder()
                                                .video(RubiconVideoParams.builder().sizeId(202).build())
                                                .accountId(321)
                                                .siteId(654)
                                                .zoneId(987)
                                                .build())))
                                .build(),
                        Imp.builder().id("impId4")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(500).h(600).build()))
                                        .build())
                                .ext((ObjectNode) mapper.createObjectNode().set("appnexus", mapper.createObjectNode()))
                                .build(),
                        Imp.builder().id("impId5")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(600).h(700).build()))
                                        .build())
                                .ext((ObjectNode) mapper.createObjectNode().set("unknown", mapper.createObjectNode()))
                                .build()))
                .tmax(1000L)
                .build();
    }

    private static BidResponse sampleBidResponse() {
        return BidResponse.builder()
                .seatbid(asList(
                        SeatBid.builder()
                                .seat("rubicon")
                                .bid(singletonList(Bid.builder()
                                        .impid("impId1")
                                        .dealid("345")
                                        .price(BigDecimal.valueOf(4.56))
                                        .w(500)
                                        .h(600)
                                        .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.of(
                                                BidType.video,
                                                singletonMap("key1", "value1"), null), null)))
                                        .build()))
                                .build(),
                        SeatBid.builder()
                                .seat("appnexus")
                                .bid(asList(
                                        Bid.builder()
                                                .impid("impId2")
                                                .dealid("456")
                                                .price(BigDecimal.valueOf(5.67))
                                                .w(600)
                                                .h(700)
                                                .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.of(
                                                        BidType.video,
                                                        singletonMap("key21", "value21"), null), null)))
                                                .build(),
                                        Bid.builder()
                                                .impid("impId2")
                                                .dealid("567")
                                                .price(BigDecimal.valueOf(6.78))
                                                .w(600)
                                                .h(700)
                                                .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.of(
                                                        BidType.video,
                                                        singletonMap("key22", "value22"), null), null)))
                                                .build()))
                                .build()))
                .ext(mapper.valueToTree(
                        ExtBidResponse.of(null, null, doubleMap("rubicon", 101, "appnexus", 202), null)))
                .build();
    }

    @SuppressWarnings("SameParameterValue")
    private static <K, V> Map<K, V> doubleMap(K key1, V value1, K key2, V value2) {
        final Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private static Event.EventBuilder expectedEventBuilderBase() {
        return Event.builder()
                .integration("pbs")
                .version("pbs-version-1")
                .client(Client.builder()
                        .deviceClass("mobile")
                        .os("os")
                        .osVersion("2.23")
                        .make("osMake")
                        .model("osModel")
                        .carrier("carrier")
                        .connectionType(17)
                        .app(org.prebid.server.rubicon.analytics.proto.App.of("bundle", "version", "sdkVersion",
                                "sdkSource"))
                        .build())
                .limitAdTracking(true)
                .eventCreator(EventCreator.of("pbsHostname", "dataCenterRegion"))
                .userAgent("userAgent");
    }

    @SuppressWarnings("SameParameterValue")
    private void givenHttpClientReturnsResponse(int statusCode, String response) {
        final HttpClientResponse httpClientResponse = HttpClientResponse.of(statusCode, null, response);
        given(httpClient.post(anyString(), any(), any(), anyLong()))
                .willReturn(Future.succeededFuture(httpClientResponse));
    }
}
