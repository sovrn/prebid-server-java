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
import static java.util.Collections.*;
import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class RubiconAnalyticsModuleTest extends VertxTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private BidderCatalog bidderCatalog;
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
        given(bidderCatalog.isValidName(anyString())).willReturn(true);
        given(bidderCatalog.usersyncerByName("rubicon")).willReturn(rubiconUsersyncer);
        given(bidderCatalog.usersyncerByName("appnexus")).willReturn(appnexusUsersyncer);

        given(rubiconUsersyncer.cookieFamilyName()).willReturn("rubicon");
        given(appnexusUsersyncer.cookieFamilyName()).willReturn("appnexus");

        given(uidsCookie.hasLiveUidFrom("rubicon")).willReturn(true);
        given(uidsCookie.hasLiveUidFrom("appnexus")).willReturn(false);

        module = new RubiconAnalyticsModule("url", 1, "pbs-version-1", "pbsHostname", "dataCenterRegion", bidderCatalog,
                httpClient);
    }

    @Test
    public void processEventShouldTakeIntoAccountSamplingFactor() {
        // given
        module = new RubiconAnalyticsModule("url", 10, "pbs-version-1", "pbsHostname", "dataCenterRegion",
                bidderCatalog, httpClient);

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
        verify(httpClient).post(anyString(), any(), anyLong());
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
    public void processEventShouldPostEventToEndpoint() throws IOException {
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
        final ArgumentCaptor<String> eventCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).post(eq("url/event"), eventCaptor.capture(), anyLong());

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
                                                        .build()))
                                        .build()),
                        1234, 1000L, true)))
                .build());
    }

    @Test
    public void postProcessShouldTakeIntoAccountSamplingFactor() {
        // given
        module = new RubiconAnalyticsModule("url", 2, "pbs-version-1", "pbsHostname", "dataCenterRegion", bidderCatalog,
                httpClient);

        final Bid bid1 = Bid.builder().build();
        final Bid bid2 = Bid.builder().build();

        // when
        module.postProcess(
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
        final BidResponse returnedBidResponse = module.postProcess(bidRequest, uidsCookie, bidResponse).result();

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
                                        .ext(mapper.valueToTree(ExtBidPrebid.of(
                                                BidType.video,
                                                singletonMap("key1", "value1"))))
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
                                                .ext(mapper.valueToTree(ExtBidPrebid.of(
                                                        BidType.video,
                                                        singletonMap("key21", "value21"))))
                                                .build(),
                                        Bid.builder()
                                                .impid("impId2")
                                                .dealid("567")
                                                .price(BigDecimal.valueOf(6.78))
                                                .w(600)
                                                .h(700)
                                                .ext(mapper.valueToTree(ExtBidPrebid.of(
                                                        BidType.video,
                                                        singletonMap("key22", "value22"))))
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
        given(httpClient.post(anyString(), any(), anyLong()))
                .willReturn(Future.succeededFuture(httpClientResponse));
    }
}
