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
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
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
import org.prebid.server.proto.openrtb.ext.request.rubicon.ExtImpRubicon;
import org.prebid.server.proto.openrtb.ext.request.rubicon.RubiconVideoParams;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebid;
import org.prebid.server.rubicon.analytics.proto.AdUnit;
import org.prebid.server.rubicon.analytics.proto.Auction;
import org.prebid.server.rubicon.analytics.proto.BidWon;
import org.prebid.server.rubicon.analytics.proto.Client;
import org.prebid.server.rubicon.analytics.proto.Dimensions;
import org.prebid.server.rubicon.analytics.proto.Error;
import org.prebid.server.rubicon.analytics.proto.Event;
import org.prebid.server.rubicon.analytics.proto.ExtApp;
import org.prebid.server.rubicon.analytics.proto.ExtAppPrebid;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Collections;

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
    private HttpClient httpClient;
    private Clock clock;

    private RubiconAnalyticsModule module;

    @Mock
    private HttpClientRequest httpClientRequest;

    @Before
    public void setUp() {
        given(httpClient.postAbs(anyString(), any())).willReturn(httpClientRequest);
        given(httpClientRequest.exceptionHandler(any())).willReturn(httpClientRequest);

        clock = Clock.fixed(LocalDateTime.of(2018, 4, 26, 15, 22).toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
        module = new RubiconAnalyticsModule("url", 1, "pbs-version-1", httpClient, clock);
    }

    @Test
    public void processEventShouldTakeIntoAccountSamplingFactor() {
        // given
        module = new RubiconAnalyticsModule("url", 10, "pbs-version-1", httpClient, clock);

        // when
        for (int i = 0; i < 10; i++) {
            module.processEvent(AuctionEvent.builder()
                    .bidRequest(BidRequest.builder()
                            .imp(emptyList())
                            .app(App.builder().build())
                            .build())
                    .bidResponse(BidResponse.builder()
                            .seatbid(emptyList())
                            .build())
                    .build());
        }

        // then
        verify(httpClient).postAbs(anyString(), any());
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
        final AuctionEvent auctionEvent = AuctionEvent.builder()
                .bidRequest(sampleBidRequest())
                .bidResponse(sampleBidResponse())
                .build();

        // when
        module.processEvent(auctionEvent);

        // then
        final ArgumentCaptor<String> eventCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient).postAbs(eq("url/event"), any());
        verify(httpClientRequest).end(eventCaptor.capture());
        then(mapper.readValue(eventCaptor.getValue(), Event.class)).isEqualTo(expectedEventBuilderBase()
                .auctions(singletonList(Auction.of(
                        "bidRequestId",
                        1,
                        asList(
                                AdUnit.builder()
                                        .transactionId("impId1")
                                        .status("success")
                                        .error(Error.of("", ""))
                                        .mediaTypes(asList("banner", "video"))
                                        .videoAdFormat("interstitial")
                                        .dimensions(asList(Dimensions.of(200, 300), Dimensions.of(300, 400)))
                                        .adUnitCode("")
                                        .adServerTargeting(singletonMap("key1", "value1"))
                                        .bids(singletonList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.of(
                                                        "rubicon",
                                                        "success",
                                                        "server",
                                                        org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                                345, BigDecimal.valueOf(4.56), "video",
                                                                Dimensions.of(500, 600)))))
                                        .build(),
                                AdUnit.builder()
                                        .transactionId("impId2")
                                        .status("success")
                                        .error(Error.of("", ""))
                                        .mediaTypes(singletonList("video"))
                                        .videoAdFormat("mid-roll")
                                        .dimensions(singletonList(Dimensions.of(100, 200)))
                                        .adUnitCode("")
                                        .adServerTargeting(singletonMap("key22", "value22"))
                                        .bids(asList(
                                                org.prebid.server.rubicon.analytics.proto.Bid.of(
                                                        "appnexus",
                                                        "success",
                                                        "server",
                                                        org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                                456, BigDecimal.valueOf(5.67), "video",
                                                                Dimensions.of(600, 700))),

                                                org.prebid.server.rubicon.analytics.proto.Bid.of(
                                                        "appnexus",
                                                        "success",
                                                        "server",
                                                        org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                                567, BigDecimal.valueOf(6.78), "video",
                                                                Dimensions.of(600, 700)))))
                                        .build()),
                        1234)))
                .build());
    }

    @Test
    public void postProcessShouldTakeIntoAccountSamplingFactor() {
        // given
        module = new RubiconAnalyticsModule("url", 2, "pbs-version-1", httpClient, clock);

        final Bid bid1 = Bid.builder().build();
        final Bid bid2 = Bid.builder().build();

        // when
        module.postProcess(
                BidRequest.builder()
                        .imp(emptyList())
                        .app(App.builder().build())
                        .build(),
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
        final BidResponse returnedBidResponse = module.postProcess(bidRequest, bidResponse).result();

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
                                        .error(Error.of("", ""))
                                        .mediaTypes(Collections.singletonList("video-instream"))
                                        .videoAdFormat("interstitial")
                                        .adUnitCode("")
                                        .source("server")
                                        .bidResponse(org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                345, BigDecimal.valueOf(4.56), "video", Dimensions.of(500, 600)))
                                        .build()))
                                .build(),
                        expectedEventBuilderBase()
                                .bidsWon(singletonList(BidWon.builder()
                                        .transactionId("impId2")
                                        .accountId(1234)
                                        .bidder("appnexus")
                                        .samplingFactor(1)
                                        .bidwonStatus("success")
                                        .error(Error.of("", ""))
                                        .mediaTypes(Collections.singletonList("video-instream"))
                                        .videoAdFormat("mid-roll")
                                        .adUnitCode("")
                                        .source("server")
                                        .bidResponse(org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                456, BigDecimal.valueOf(5.67), "video", Dimensions.of(600, 700)))
                                        .build()))
                                .build(),
                        expectedEventBuilderBase()
                                .bidsWon(singletonList(BidWon.builder()
                                        .transactionId("impId2")
                                        .accountId(1234)
                                        .bidder("appnexus")
                                        .samplingFactor(1)
                                        .bidwonStatus("success")
                                        .error(Error.of("", ""))
                                        .mediaTypes(Collections.singletonList("video-instream"))
                                        .videoAdFormat("mid-roll")
                                        .adUnitCode("")
                                        .source("server")
                                        .bidResponse(org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                                                567, BigDecimal.valueOf(6.78), "video", Dimensions.of(600, 700)))
                                        .build()))
                                .build());
    }

    private BidRequest sampleBidRequest() {
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
                                                .build())))
                                .build(),
                        Imp.builder().id("impId2")
                                .video(Video.builder().startdelay(-1).w(100).h(200).build())
                                .build()))
                .build();
    }

    private BidResponse sampleBidResponse() {
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
                .build();
    }

    private Event.EventBuilder expectedEventBuilderBase() {
        return Event.builder()
                .eventTimeMillis(clock.millis())
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
                .limitAdTracking(true);
    }
}