package org.prebid.server.rubicon.analytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import lombok.Value;
import org.prebid.server.analytics.AnalyticsReporter;
import org.prebid.server.analytics.model.AmpEvent;
import org.prebid.server.analytics.model.AuctionEvent;
import org.prebid.server.auction.BidResponsePostProcessor;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.cache.proto.response.BidCacheResponse;
import org.prebid.server.cookie.UidsCookie;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.exception.PreBidException;
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
import org.prebid.server.rubicon.analytics.proto.StartDelay;
import org.prebid.server.rubicon.analytics.proto.VideoAdFormat;
import org.prebid.server.vertx.http.HttpClient;
import org.prebid.server.vertx.http.model.HttpClientResponse;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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

    private static final TypeReference<ExtPrebid<ExtBidPrebid, ObjectNode>> EXT_PREBID_TYPE_REFERENCE =
            new TypeReference<ExtPrebid<ExtBidPrebid, ObjectNode>>() {
            };

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
    private final String pbsHostname;
    private final String dataCenterRegion;
    private final BidderCatalog bidderCatalog;
    private final UidsCookieService uidsCookieService;
    private final HttpClient httpClient;

    private long auctionEventCount = 0;
    private long ampEventCount = 0;
    private long bidWonCount = 0;

    public RubiconAnalyticsModule(String endpointUrl, int samplingFactor, String pbsVersion, String pbsHostname,
                                  String dataCenterRegion, BidderCatalog bidderCatalog,
                                  UidsCookieService uidsCookieService, HttpClient httpClient) {
        this.endpointUrl = Objects.requireNonNull(endpointUrl) + EVENT_PATH;
        this.samplingFactor = samplingFactor;
        this.pbsVersion = pbsVersion;
        this.pbsHostname = Objects.requireNonNull(pbsHostname);
        this.dataCenterRegion = Objects.requireNonNull(dataCenterRegion);
        this.bidderCatalog = Objects.requireNonNull(bidderCatalog);
        this.uidsCookieService = Objects.requireNonNull(uidsCookieService);
        this.httpClient = Objects.requireNonNull(httpClient);
    }

    @Override
    public <T> void processEvent(T event) {
        if (event instanceof AuctionEvent) {
            processAuctionEvent((AuctionEvent) event);
        } else if (event instanceof AmpEvent) {
            processAmpEvent((AmpEvent) event);
        }
    }

    @Override
    public Future<BidResponse> postProcess(RoutingContext context, BidRequest bidRequest, UidsCookie uidsCookie,
                                           BidResponse bidResponse) {
        return Future.succeededFuture(processBidResponse(context, bidRequest, uidsCookie, bidResponse));
    }

    private BidResponse processBidResponse(RoutingContext context, BidRequest bidRequest,
                                           UidsCookie uidsCookie, BidResponse bidResponse) {
        if (bidRequest.getApp() != null) {
            final ExtBidResponse extBidResponse = readExt(bidResponse.getExt(), ExtBidResponse.class);
            final boolean hasRubiconId = uidsCookieService.parseHostCookie(context) != null;

            for (final SeatBid seatBid : bidResponse.getSeatbid()) {
                final String bidder = seatBid.getSeat();
                final Integer responseTime = serverLatencyMillisFrom(extBidResponse, bidder);
                final Boolean serverHasUserId = serverHasUserIdFrom(uidsCookie, bidder);

                for (final Bid bid : seatBid.getBid()) {
                    // only continue if counter matches sampling factor
                    if (++bidWonCount % samplingFactor == 0) {
                        addEventCallbackToBid(bid,
                                toBidWonEvent(bidRequest, bidder, responseTime, serverHasUserId, hasRubiconId, bid));
                    }
                }
            }
        }

        return bidResponse;
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

    private void processAuctionEvent(AuctionEvent event) {
        final BidRequest bidRequest = event.getBidRequest();
        final UidsCookie uidsCookie = event.getUidsCookie();
        final BidResponse bidResponse = event.getBidResponse();

        // only send event for mobile requests
        if (bidRequest == null || uidsCookie == null || bidResponse == null || bidRequest.getApp() == null) {
            return;
        }

        // only continue if counter matches sampling factor
        if (++auctionEventCount % samplingFactor == 0) {
            final List<AdUnit> adUnits = toAdUnits(bidRequest, uidsCookie, bidResponse);
            final Integer accountId = accountIdFromApp(bidRequest);
            postEvent(toAuctionEvent(bidRequest, uidsCookie, adUnits, accountId, this::eventBuilderBaseFromDevice));
        }
    }

    private void processAmpEvent(AmpEvent ampEvent) {
        final BidRequest bidRequest = ampEvent.getBidRequest();
        final UidsCookie uidsCookie = ampEvent.getUidsCookie();
        final BidResponse bidResponse = ampEvent.getBidResponse();

        if (bidRequest == null || uidsCookie == null || bidResponse == null || bidRequest.getSite() == null) {
            return;
        }

        // only continue if counter matches sampling factor
        if (++ampEventCount % samplingFactor == 0) {
            final List<AdUnit> adUnits = toAdUnits(bidRequest, uidsCookie, bidResponse);
            final Integer accountId = accountIdFromSite(bidRequest);
            postEvent(toAuctionEvent(bidRequest, uidsCookie, adUnits, accountId, this::eventBuilderBaseFromSite));
        }
    }

    private List<AdUnit> toAdUnits(BidRequest bidRequest, UidsCookie uidsCookie, BidResponse bidResponse) {
        final Map<String, List<TwinBids>> impIdToBids = toBidsByImpId(bidRequest, uidsCookie, bidResponse);

        return bidRequest.getImp().stream()
                .map(imp -> toAdUnit(imp, impIdToBids.getOrDefault(imp.getId(), Collections.emptyList())))
                .collect(Collectors.toList());
    }

    private Map<String, List<TwinBids>> toBidsByImpId(BidRequest bidRequest, UidsCookie uidsCookie,
                                                      BidResponse bidResponse) {
        final ExtBidResponse extBidResponse = readExt(bidResponse.getExt(), ExtBidResponse.class);
        final Map<String, List<TwinBids>> impIdToBids = new HashMap<>();

        populateSuccessfulBids(bidRequest, uidsCookie, bidResponse, extBidResponse, impIdToBids);
        populateFailedBids(bidRequest, uidsCookie, extBidResponse, impIdToBids);

        return impIdToBids;
    }

    private void populateSuccessfulBids(BidRequest bidRequest, UidsCookie uidsCookie, BidResponse bidResponse,
                                        ExtBidResponse extBidResponse, Map<String, List<TwinBids>> impIdToBids) {
        for (final SeatBid seatBid : bidResponse.getSeatbid()) {
            final String bidder = seatBid.getSeat();
            final Integer responseTime = serverLatencyMillisFrom(extBidResponse, bidder);
            final Boolean serverHasUserId = serverHasUserIdFrom(uidsCookie, bidder);

            for (final Bid bid : seatBid.getBid()) {
                impIdToBids.computeIfAbsent(bid.getImpid(), key -> new ArrayList<>())
                        .add(toTwinBids(bidder, findImpById(bidRequest.getImp(), bid.getImpid()),
                                bid, responseTime, serverHasUserId));
            }
        }
    }

    private void populateFailedBids(BidRequest bidRequest, UidsCookie uidsCookie, ExtBidResponse extBidResponse,
                                    Map<String, List<TwinBids>> impIdToBids) {
        for (Imp imp : bidRequest.getImp()) {
            final ObjectNode impExt = imp.getExt();
            if (impExt == null) {
                continue;
            }

            final String impId = imp.getId();
            final Iterator<String> bidderIterator = impExt.fieldNames();
            while (bidderIterator.hasNext()) {
                final String bidder = bidderIterator.next();
                if (analyticsBidExists(impIdToBids, impId, bidder)) {
                    continue;
                }

                final Integer responseTime = serverLatencyMillisFrom(extBidResponse, bidder);
                final Boolean serverHasUserId = serverHasUserIdFrom(uidsCookie, bidder);

                impIdToBids.computeIfAbsent(impId, key -> new ArrayList<>())
                        .add(toTwinBids(bidder, imp, null, responseTime, serverHasUserId));
            }
        }
    }

    private static boolean analyticsBidExists(Map<String, List<TwinBids>> impIdToBids, String impId, String bidder) {
        return impIdToBids.containsKey(impId) && impIdToBids.get(impId).stream()
                .anyMatch(twinBids -> Objects.equals(bidder, twinBids.getAnalyticsBid().getBidder()));
    }

    private static Integer serverLatencyMillisFrom(ExtBidResponse extBidResponse, String bidder) {
        return extBidResponse != null
                ? extBidResponse.getResponsetimemillis().get(bidder)
                : null;
    }

    private Boolean serverHasUserIdFrom(UidsCookie uidsCookie, String bidder) {
        return bidderCatalog.isValidName(bidder)
                ? uidsCookie.hasLiveUidFrom(bidderCatalog.usersyncerByName(bidder).cookieFamilyName())
                : null;
    }

    private static Imp findImpById(List<Imp> imps, String impId) {
        return imps.stream()
                .filter(imp -> Objects.equals(imp.getId(), impId))
                .findFirst()
                .orElse(null);
    }

    private static TwinBids toTwinBids(String bidder, Imp imp, Bid bid, Integer serverLatencyMillis,
                                       Boolean serverHasUserId) {

        final org.prebid.server.rubicon.analytics.proto.Bid analyticsBid =
                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                        .bidder(bidder)
                        .status(bid != null ? SUCCESS_STATUS : NO_BID_STATUS)
                        .source(SERVER_SOURCE)
                        .serverLatencyMillis(serverLatencyMillis)
                        .serverHasUserId(serverHasUserId)
                        .params(paramsFrom(imp, bidder))
                        .bidResponse(analyticsBidResponse(bid, mediaTypeString(mediaTypeFromBid(bid))))
                        .build();

        return new TwinBids(bid, analyticsBid);
    }

    private static Params paramsFrom(Imp imp, String bidder) {
        final Params result;
        if (imp != null && Objects.equals(bidder, RUBICON_BIDDER)) {
            // it should be safe to cast since there wouldn't be rubicon bids if this imp had no "rubicon" field in ext
            final ExtImpRubicon impExt = readExt((ObjectNode) imp.getExt().get(RUBICON_BIDDER), ExtImpRubicon.class);

            result = impExt != null ? Params.of(impExt.getAccountId(), impExt.getSiteId(), impExt.getZoneId()) : null;
        } else {
            result = null;
        }
        return result;
    }

    private static BidType mediaTypeFromBid(Bid bid) {
        final ExtPrebid<ExtBidPrebid, ObjectNode> extBid = bid != null ? readExtPrebid(bid.getExt()) : null;
        final ExtBidPrebid extBidPrebid = extBid != null ? extBid.getPrebid() : null;
        return extBidPrebid != null ? extBidPrebid.getType() : null;
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
            logger.warn("Error unmarshalling ext by class {0}", e, type);
            return null;
        }
    }

    private static ExtPrebid<ExtBidPrebid, ObjectNode> readExtPrebid(ObjectNode ext) {
        if (ext == null) {
            return null;
        }

        try {
            return Json.mapper.convertValue(ext, EXT_PREBID_TYPE_REFERENCE);
        } catch (IllegalArgumentException e) {
            logger.warn("Error unmarshalling ext by type reference {0}", e, EXT_PREBID_TYPE_REFERENCE);
            return null;
        }
    }

    private static org.prebid.server.rubicon.analytics.proto.BidResponse analyticsBidResponse(
            Bid bid, String mediaType) {

        return bid != null
                ? org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                parseId(bid.getDealid()),
                // TODO will need to convert currencies to USD once currency support has been added
                bid.getPrice(),
                mediaType,
                Dimensions.of(bid.getW(), bid.getH()))
                : null;
    }

    private static AdUnit toAdUnit(Imp imp, List<TwinBids> bids) {
        final boolean openrtbBidsFound = bids.stream().map(TwinBids::getOpenrtbBid).anyMatch(Objects::nonNull);

        return AdUnit.builder()
                .transactionId(imp.getId())
                .status(openrtbBidsFound ? SUCCESS_STATUS : NO_BID_STATUS)
                .error(null) // we do not have insight into any bid errors from here
                .mediaTypes(mediaTypesFromImp(imp))
                .videoAdFormat(imp.getVideo() != null ? videoAdFormatFromImp(imp, bids) : null)
                .dimensions(dimensions(imp))
                .adUnitCode(null) // does not apply to mobile ads
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

    private static String videoAdFormatFromImp(Imp imp, String bidder) {
        return imp != null ? videoAdFormat(imp, Objects.equals(bidder, RUBICON_BIDDER)) : null;
    }

    private static boolean isRubiconVideoBid(org.prebid.server.rubicon.analytics.proto.Bid bid) {
        return Objects.equals(bid.getBidder(), RUBICON_BIDDER)
                && bid.getBidResponse() != null
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
        final Bid openrtbBid = bid.getOpenrtbBid();
        final ExtPrebid<ExtBidPrebid, ObjectNode> extBid = openrtbBid != null
                ? readExtPrebid(openrtbBid.getExt())
                : null;
        return extBid != null ? extBid.getPrebid().getTargeting() : null;
    }

    private Event toAuctionEvent(BidRequest bidRequest, UidsCookie uidsCookie, List<AdUnit> adUnits,
                                 Integer accountId, Function<BidRequest, Event.EventBuilder> eventBuilderBase) {
        return eventBuilderBase.apply(bidRequest)
                .auctions(Collections.singletonList(Auction.of(
                        bidRequest.getId(),
                        samplingFactor,
                        adUnits,
                        accountId,
                        bidRequest.getTmax(),
                        serverHasUserIdFrom(uidsCookie, RUBICON_BIDDER))))
                .build();
    }

    private Event toBidWonEvent(BidRequest bidRequest, String bidder, Integer serverLatencyMillis,
                                Boolean serverHasUserId, boolean hasRubiconId, Bid bid) {
        final Imp foundImp = findImpById(bidRequest.getImp(), bid.getImpid());
        final BidType bidType = mediaTypeFromBid(bid);
        final String bidTypeString = mediaTypeString(bidType);

        return eventBuilderBaseFromDevice(bidRequest)
                .bidsWon(Collections.singletonList(BidWon.builder()
                        .transactionId(bid.getImpid())
                        .accountId(accountIdFromApp(bidRequest))
                        .bidder(bidder)
                        .samplingFactor(samplingFactor)
                        .bidwonStatus(SUCCESS_STATUS)
                        .mediaTypes(mediaTypesForBidWonFromImp(foundImp))
                        .videoAdFormat(bidType == BidType.video ? videoAdFormatFromImp(foundImp, bidder) : null)
                        // we do not have insight into any errors that might occur since the url is not
                        // fired until creative is rendered
                        .error(null)
                        .adUnitCode(null) // does not apply to mobile ads
                        .source(SERVER_SOURCE)
                        .bidResponse(analyticsBidResponse(bid, bidTypeString))
                        .serverLatencyMillis(serverLatencyMillis)
                        .serverHasUserId(serverHasUserId)
                        .hasRubiconId(hasRubiconId)
                        .params(paramsFrom(foundImp, bidder))
                        .build()))
                .build();
    }

    private static Integer accountIdFromApp(BidRequest bidRequest) {
        final Publisher publisher = bidRequest.getApp().getPublisher();
        return publisher != null ? parseId(publisher.getId()) : null;
    }

    private static Integer accountIdFromSite(BidRequest bidRequest) {
        final Publisher publisher = bidRequest.getSite().getPublisher();
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

    private static List<String> mediaTypesForBidWonFromImp(Imp imp) {
        final List<String> mediaTypes;
        if (imp != null) {
            mediaTypes = new ArrayList<>();
            if (imp.getBanner() != null) {
                mediaTypes.add(BidType.banner.name());
            }
            if (imp.getVideo() != null) {
                mediaTypes.add("video-instream");
            }
            if (imp.getXNative() != null) {
                mediaTypes.add("native");
            }
        } else {
            mediaTypes = Collections.emptyList();
        }
        return mediaTypes;
    }

    /**
     * Prepares event from request from mobile app.
     */
    private Event.EventBuilder eventBuilderBaseFromDevice(BidRequest bidRequest) {
        final App app = bidRequest.getApp();
        final ExtApp appExt = readExt(app.getExt(), ExtApp.class);
        final ExtAppPrebid appExtPrebid = appExt != null ? appExt.getPrebid() : null;

        final Device device = bidRequest.getDevice();
        final Integer deviceLmt = getIfNotNull(device, Device::getLmt);

        return Event.builder()
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
                .limitAdTracking(deviceLmt != null ? deviceLmt != 0 : null)
                .eventCreator(EventCreator.of(pbsHostname, dataCenterRegion))
                .userAgent(getIfNotNull(device, Device::getUa));
    }

    /**
     * Prepares event from request from mobile web.
     */
    private Event.EventBuilder eventBuilderBaseFromSite(BidRequest bidRequest) {
        final Device device = bidRequest.getDevice();
        final Integer deviceLmt = getIfNotNull(device, Device::getLmt);

        return Event.builder()
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
                        .build())
                .limitAdTracking(deviceLmt != null ? deviceLmt != 0 : null)
                .eventCreator(EventCreator.of(pbsHostname, dataCenterRegion))
                .userAgent(getIfNotNull(device, Device::getUa))
                .referrerUri(getIfNotNull(bidRequest.getSite(), Site::getPage));
    }

    private static <T, R> R getIfNotNull(T target, Function<T, R> getter) {
        return target != null ? getter.apply(target) : null;
    }

    /**
     * Sends event to analytics service.
     */
    private void postEvent(Event event) {
        httpClient.post(endpointUrl, headers(event), Json.encode(event), 2000L)
                .compose(RubiconAnalyticsModule::processResponse)
                .recover(RubiconAnalyticsModule::failResponse);
    }

    /**
     * Returns headers needed for analytic request.
     */
    private static MultiMap headers(Event event) {
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
        final String userAgent = event.getUserAgent();
        if (userAgent != null) {
            headers.add(HttpHeaders.USER_AGENT, userAgent);
        }
        return headers;
    }

    /**
     * Handles {@link HttpClientResponse}, analyzes response status
     * and creates {@link Future} with {@link BidCacheResponse} from body content
     * or throws {@link PreBidException} in case of errors.
     */
    private static Future<Void> processResponse(HttpClientResponse response) {
        final int statusCode = response.getStatusCode();
        if (statusCode != 200) {
            throw new PreBidException(String.format("HTTP status code %d", statusCode));
        }
        return Future.succeededFuture();
    }

    /**
     * Handles errors occurred while HTTP request or response processing.
     */
    private static Future<Void> failResponse(Throwable exception) {
        logger.warn("Error occurred while interacting with Rubicon Analytics", exception);
        return Future.failedFuture(exception);
    }

    /**
     * Holds correspondence between OpenRTB Bid and Analytics Bid.
     */
    @Value
    private static final class TwinBids {

        Bid openrtbBid;

        org.prebid.server.rubicon.analytics.proto.Bid analyticsBid;
    }
}
