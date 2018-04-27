package org.prebid.server.rubicon.analytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Device;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import lombok.Value;
import org.apache.commons.collections4.CollectionUtils;
import org.prebid.server.analytics.AnalyticsReporter;
import org.prebid.server.analytics.model.AuctionEvent;
import org.prebid.server.auction.BidResponsePostProcessor;
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
import org.prebid.server.rubicon.analytics.proto.StartDelay;
import org.prebid.server.rubicon.analytics.proto.VideoAdFormat;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RubiconAnalyticsModule implements AnalyticsReporter, BidResponsePostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RubiconAnalyticsModule.class);

    private static final String EVENT_PATH = "/event";

    private static final String SUCCESS_STATUS = "success";
    private static final String NO_BID_STATUS = "no-bid";

    private static final String RUBICON_BIDDER = "rubicon";

    private static final String SERVER_SOURCE = "server";

    private static final String PBS_INTEGRATION = "pbs";

    private static final String MOBILE_DEVICE_CLASS = "mobile";

    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private static final Map<Integer, String> VIDEO_SIZE_AD_FORMATS;

    static {
        VIDEO_SIZE_AD_FORMATS = new HashMap<>();
        VIDEO_SIZE_AD_FORMATS.put(201, VideoAdFormat.PREROLL);
        VIDEO_SIZE_AD_FORMATS.put(202, "interstitial");
        VIDEO_SIZE_AD_FORMATS.put(203, "outstream");
        VIDEO_SIZE_AD_FORMATS.put(204, VideoAdFormat.MIDROLL);
        VIDEO_SIZE_AD_FORMATS.put(205, VideoAdFormat.POSTROLL);
        VIDEO_SIZE_AD_FORMATS.put(207, "vertical");
    }

    private final String endpointUrl;
    private final int samplingFactor;
    private final String pbsVersion;

    private HttpClient httpClient;
    private final Clock clock;

    private long auctionEventCount = 0;
    private long bidWonCount = 0;

    public RubiconAnalyticsModule(String endpointUrl, int samplingFactor, String pbsVersion, HttpClient httpClient,
                                  Clock clock) {
        this.endpointUrl = Objects.requireNonNull(endpointUrl) + EVENT_PATH;
        this.samplingFactor = samplingFactor;
        this.pbsVersion = pbsVersion;
        this.httpClient = Objects.requireNonNull(httpClient);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public <T> void processEvent(T event) {
        if (event instanceof AuctionEvent) {
            processAuctionEvent((AuctionEvent) event);
        }
    }

    @Override
    public Future<BidResponse> postProcess(BidRequest bidRequest, BidResponse bidResponse) {
        return Future.succeededFuture(processBidResponse(bidRequest, bidResponse));
    }

    private void processAuctionEvent(AuctionEvent event) {
        // only send event for mobile requests
        final BidRequest bidRequest = event.getBidRequest();
        if (bidRequest == null || event.getBidResponse() == null || bidRequest.getApp() == null) {
            return;
        }

        // only continue if counter matches sampling factor
        if (++auctionEventCount % samplingFactor == 0) {
            sendAuctionData(event);
        }
    }

    private BidResponse processBidResponse(BidRequest bidRequest, BidResponse bidResponse) {
        if (bidRequest.getApp() != null) {
            for (final SeatBid seatBid : bidResponse.getSeatbid()) {
                final String bidder = seatBid.getSeat();

                for (final Bid bid : seatBid.getBid()) {
                    // only continue if counter matches sampling factor
                    if (++bidWonCount % samplingFactor == 0) {
                        addEventCallbackToBid(bid, toBidWonEvent(bidRequest, bidder, bid));
                    }
                }
            }
        }

        return bidResponse;
    }

    private void sendAuctionData(AuctionEvent auctionEvent) {
        final BidRequest bidRequest = auctionEvent.getBidRequest();

        postEvent(toAuctionEvent(bidRequest, toAdUnits(bidRequest, auctionEvent.getBidResponse())));
    }

    private static Map<String, List<TwinBids>> toBidsByImpId(BidResponse bidResponse) {

        final Map<String, List<TwinBids>> impIdToBidsMap = new HashMap<>();

        for (final SeatBid seatBid : bidResponse.getSeatbid()) {
            for (final Bid bid : seatBid.getBid()) {
                impIdToBidsMap.computeIfAbsent(bid.getImpid(), key -> new ArrayList<>()).add(toBid(bid, seatBid));
            }
        }

        return impIdToBidsMap;
    }

    private static TwinBids toBid(Bid bid, SeatBid seatBid) {
        final org.prebid.server.rubicon.analytics.proto.Bid analyticsBid =
                org.prebid.server.rubicon.analytics.proto.Bid.of(
                        seatBid.getSeat(),
                        SUCCESS_STATUS,
                        SERVER_SOURCE,
                        analyticsBidResponse(bid, mediaTypeString(mediaTypeFromBid(bid))));

        return new TwinBids(bid, analyticsBid);
    }

    private static BidType mediaTypeFromBid(Bid bid) {
        final ExtBidPrebid bidExt = readExt(bid.getExt(), ExtBidPrebid.class);
        return bidExt != null ? bidExt.getType() : null;
    }

    private static String mediaTypeString(BidType bidType) {
        return bidType != null ? bidType.name() : null;
    }

    private static <T> T readExt(ObjectNode ext, Class<T> type) {
        if (ext == null) {
            return null;
        }

        try {
            return Json.mapper.treeToValue(ext, type);
        } catch (JsonProcessingException e) {
            logger.warn("Error unmarshalling ext", e);
            return null;
        }
    }

    private static org.prebid.server.rubicon.analytics.proto.BidResponse analyticsBidResponse(
            Bid bid, String mediaType) {

        return org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                parseId(bid.getDealid()),
                // TODO will need to convert currencies to USD once currency support has
                // been added
                bid.getPrice(),
                mediaType,
                Dimensions.of(bid.getW(), bid.getH()));
    }

    private static List<AdUnit> toAdUnits(BidRequest bidRequest, BidResponse bidResponse) {
        final Map<String, List<TwinBids>> impIdToBidsMap = toBidsByImpId(bidResponse);

        return bidRequest.getImp().stream()
                .map(imp -> toAdUnit(imp, impIdToBidsMap.getOrDefault(imp.getId(), Collections.emptyList())))
                .collect(Collectors.toList());
    }

    private static AdUnit toAdUnit(Imp imp, List<TwinBids> bids) {
        return AdUnit.builder()
                .transactionId(imp.getId())
                .status(CollectionUtils.isNotEmpty(bids) ? SUCCESS_STATUS : NO_BID_STATUS)
                .error(Error.of("", "")) // we do not have insight into any bid errors from here
                .mediaTypes(mediaTypesFromImp(imp))
                .videoAdFormat(imp.getVideo() != null ? videoAdFormatFromImp(imp, bids) : null)
                .dimensions(dimensions(imp))
                .adUnitCode("") // does not apply to mobile ads
                .adServerTargeting(targetingForImp(bids))
                .bids(bids.stream().map(TwinBids::getAnalyticsBid).collect(Collectors.toList()))
                .build();
    }

    private static List<String> mediaTypesFromImp(Imp imp) {
        final List<String> mediaTypes = new ArrayList<>();
        if (imp.getBanner() != null) {
            mediaTypes.add(BidType.banner.name());
        }
        if (imp.getVideo() != null) {
            mediaTypes.add(BidType.video.name());
        }
        return mediaTypes;
    }

    private static String videoAdFormatFromImp(Imp imp, List<TwinBids> bids) {
        final boolean hasRubiconBid = bids.stream()
                .map(TwinBids::getAnalyticsBid)
                .anyMatch(RubiconAnalyticsModule::isRubiconVideoBid);

        return videoAdFormat(imp, hasRubiconBid);
    }

    private static String videoAdFormatFromImps(List<Imp> imps, Bid bid, String bidder) {
        return imps.stream()
                .filter(imp -> Objects.equals(imp.getId(), bid.getImpid()))
                .findFirst()
                .map(imp -> videoAdFormat(imp, Objects.equals(bidder, RUBICON_BIDDER)))
                .orElse(null);
    }

    private static boolean isRubiconVideoBid(org.prebid.server.rubicon.analytics.proto.Bid bid) {
        return Objects.equals(bid.getBidder(), RUBICON_BIDDER)
                && Objects.equals(bid.getBidResponse().getMediaType(), BidType.video.name());
    }

    private static String videoAdFormat(Imp imp, boolean hasRubiconBid) {
        final Integer startdelay = imp.getVideo().getStartdelay();

        if (hasRubiconBid) {
            // it should be safe to cast since there wouldn't be rubicon bids if this imp had no "rubicon" field in ext
            final ExtImpRubicon impExt = readExt((ObjectNode) imp.getExt().get(RUBICON_BIDDER), ExtImpRubicon.class);
            final RubiconVideoParams videoParams = impExt != null ? impExt.getVideo() : null;
            if (videoParams != null) {
                return VIDEO_SIZE_AD_FORMATS.get(videoParams.getSizeId());
            }
        } else if (startdelay != null) {
            if (startdelay == StartDelay.GENERIC_MIDROLL || startdelay > StartDelay.PREROLL) {
                return VideoAdFormat.MIDROLL;
            } else if (startdelay == StartDelay.GENERIC_POSTROLL) {
                return VideoAdFormat.POSTROLL;
            } else {
                return VideoAdFormat.PREROLL;
            }
        }

        return null;
    }

    private static List<Dimensions> dimensions(Imp imp) {
        final Banner banner = imp.getBanner();
        final List<Format> bannerFormat = banner != null ? banner.getFormat() : null;
        final Video video = imp.getVideo();

        if (bannerFormat != null) {
            return bannerFormat.stream().map(f -> Dimensions.of(f.getW(), f.getH())).collect(Collectors.toList());
        } else if (video != null) {
            return Collections.singletonList(Dimensions.of(video.getW(), video.getH()));
        } else {
            return null;
        }
    }

    private static Map<String, String> targetingForImp(List<TwinBids> bids) {
        return bids.stream()
                .map(RubiconAnalyticsModule::targetingFromBid)
                .filter(Objects::nonNull)
                .reduce((first, second) -> second)
                .orElse(null);
    }

    private static Map<String, String> targetingFromBid(TwinBids bid) {
        final ExtBidPrebid bidExt = readExt(bid.getOpenrtbBid().getExt(), ExtBidPrebid.class);

        return bidExt != null ? bidExt.getTargeting() : null;
    }

    private Event toAuctionEvent(BidRequest bidRequest, List<AdUnit> adUnits) {
        return eventBuilderBase(bidRequest)
                .auctions(Collections.singletonList(Auction.of(
                        bidRequest.getId(),
                        samplingFactor,
                        adUnits,
                        accountId(bidRequest))))
                .build();
    }

    private Event toBidWonEvent(BidRequest bidRequest, String bidder, Bid bid) {
        final BidType bidType = mediaTypeFromBid(bid);
        final boolean isVideo = bidType == BidType.video;
        final String bidTypeString = mediaTypeString(bidType);

        return eventBuilderBase(bidRequest)
                .bidsWon(Collections.singletonList(BidWon.builder()
                        .transactionId(bid.getImpid())
                        .accountId(accountId(bidRequest))
                        .bidder(bidder)
                        .samplingFactor(samplingFactor)
                        .bidwonStatus(SUCCESS_STATUS)
                        // we do not have insight into any errors that might occur since the url is not
                        // fired until creative is rendered
                        .error(Error.of("", ""))
                        .mediaTypes(Collections.singletonList(isVideo ? "video-instream" : bidTypeString))
                        .videoAdFormat(isVideo ? videoAdFormatFromImps(bidRequest.getImp(), bid, bidder) : null)
                        .adUnitCode("") // does not apply to mobile ads
                        .source(SERVER_SOURCE)
                        .bidResponse(analyticsBidResponse(bid, bidTypeString))
                        .build()))
                .build();
    }

    private static Integer accountId(BidRequest bidRequest) {
        final Publisher publisher = bidRequest.getApp().getPublisher();
        return publisher != null ? parseId(publisher.getId()) : null;
    }

    private static Integer parseId(String id) {
        if (id == null) {
            return null;
        }

        try {
            return Integer.parseInt(id);
        } catch (NumberFormatException e) {
            logger.warn("Id [{0}] is not a number", id);
            return null;
        }
    }

    private Event.EventBuilder eventBuilderBase(BidRequest bidRequest) {
        final App app = bidRequest.getApp();

        final ExtApp appExt = readExt(app.getExt(), ExtApp.class);
        final ExtAppPrebid appExtPrebid = appExt != null ? appExt.getPrebid() : null;

        final Device device = bidRequest.getDevice();
        final Integer deviceLmt = getIfNotNull(device, Device::getLmt);

        return Event.builder()
                .eventTimeMillis(clock.millis())
                .integration(PBS_INTEGRATION)
                .version(pbsVersion)
                .client(Client.builder()
                        .deviceClass(MOBILE_DEVICE_CLASS)
                        .os(getIfNotNull(device, Device::getOs))
                        .osVersion(getIfNotNull(device, Device::getOsv))
                        .make(getIfNotNull(device, Device::getMake))
                        .model(getIfNotNull(device, Device::getModel))
                        .carrier(getIfNotNull(device, Device::getCarrier))
                        .connectionType(getIfNotNull(device, Device::getConnectiontype))
                        .app(org.prebid.server.rubicon.analytics.proto.App.of(
                                app.getBundle(),
                                app.getVer(),
                                getIfNotNull(appExtPrebid, ExtAppPrebid::getVersion),
                                getIfNotNull(appExtPrebid, ExtAppPrebid::getSource)))
                        .build())
                .limitAdTracking(deviceLmt != null ? deviceLmt != 0 : null);
    }

    private static <T, R> R getIfNotNull(T target, Function<T, R> getter) {
        return target != null ? getter.apply(target) : null;
    }

    private void postEvent(Event event) {
        httpClient.postAbs(endpointUrl, RubiconAnalyticsModule::handleResponse)
                .exceptionHandler(RubiconAnalyticsModule::handleException)
                .end(Json.encode(event));
    }

    private static void handleResponse(HttpClientResponse response) {
        if (response.statusCode() != HttpResponseStatus.ACCEPTED.code()) {
            logger.warn("Unexpected status after sending auction event: {0}", response.statusCode());
        }
    }

    private static void handleException(Throwable throwable) {
        logger.warn("Exception occurred while sending analytics payload", throwable);
    }

    private void addEventCallbackToBid(Bid bid, Event event) {
        final byte[] eventBytes;
        try {
            eventBytes = Json.mapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            // not expected to happen though
            logger.warn("Exception occurred while marshalling analytics event", e);
            return;
        }

        final String encodedEvent = BASE64_ENCODER.encodeToString(eventBytes);
        final String url = String.format("%s?type=bidWon&data=%s", endpointUrl, encodedEvent);

        bid.setNurl(url);
    }

    @Value
    private static final class TwinBids {

        Bid openrtbBid;

        org.prebid.server.rubicon.analytics.proto.Bid analyticsBid;
    }
}
