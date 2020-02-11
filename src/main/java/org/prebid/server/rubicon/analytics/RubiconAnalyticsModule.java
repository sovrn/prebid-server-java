package org.prebid.server.rubicon.analytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Device;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Geo;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import lombok.Value;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.prebid.server.analytics.AnalyticsReporter;
import org.prebid.server.analytics.model.AmpEvent;
import org.prebid.server.analytics.model.AuctionEvent;
import org.prebid.server.analytics.model.HttpContext;
import org.prebid.server.analytics.model.NotificationEvent;
import org.prebid.server.auction.BidResponsePostProcessor;
import org.prebid.server.auction.model.AuctionContext;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.cookie.UidsCookie;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
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
import org.prebid.server.rubicon.analytics.proto.Event;
import org.prebid.server.rubicon.analytics.proto.EventCreator;
import org.prebid.server.rubicon.analytics.proto.ExtApp;
import org.prebid.server.rubicon.analytics.proto.ExtAppPrebid;
import org.prebid.server.rubicon.analytics.proto.ExtRequest;
import org.prebid.server.rubicon.analytics.proto.ExtRequestPrebid;
import org.prebid.server.rubicon.analytics.proto.ExtRequestPrebidBidders;
import org.prebid.server.rubicon.analytics.proto.ExtRequestPrebidBiddersRubicon;
import org.prebid.server.rubicon.analytics.proto.Impression;
import org.prebid.server.rubicon.analytics.proto.Params;
import org.prebid.server.rubicon.analytics.proto.StartDelay;
import org.prebid.server.rubicon.analytics.proto.VideoAdFormat;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.rubicon.audit.proto.UidAudit;
import org.prebid.server.settings.model.Account;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.vertx.http.HttpClient;
import org.prebid.server.vertx.http.model.HttpClientResponse;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RubiconAnalyticsModule implements AnalyticsReporter, BidResponsePostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RubiconAnalyticsModule.class);

    private static final String REFERER_HEADER = "Referer";
    private static final String DNT_HEADER = "DNT";
    private static final String USER_AGENT_HEADER = "User-Agent";

    private static final String EVENT_PATH = "/event";

    private static final String PREBID_EXT = "prebid";

    private static final String SUCCESS_STATUS = "success";
    private static final String NO_BID_STATUS = "no-bid";
    private static final String ERROR_STATUS = "error";

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

    private static final String APPLICATION_JSON =
            HttpHeaderValues.APPLICATION_JSON.toString() + ";" + HttpHeaderValues.CHARSET.toString() + "=" + "utf-8";

    private final String endpointUrl;
    private final String pbsVersion;
    private final Integer globalSamplingFactor;
    private final String pbsHostname;
    private final String dataCenterRegion;
    private final BidderCatalog bidderCatalog;
    private final UidsCookieService uidsCookieService;
    private final UidsAuditCookieService uidsAuditCookieService;
    private final HttpClient httpClient;
    private final JacksonMapper mapper;

    private AtomicLong auctionEventCount = new AtomicLong();
    private AtomicLong ampEventCount = new AtomicLong();
    private AtomicLong bidWonCount = new AtomicLong();

    private Map<Integer, Long> accountToAuctionEventCount = new ConcurrentHashMap<>();
    private Map<Integer, Long> accountToAmpEventCount = new ConcurrentHashMap<>();
    private Map<Integer, Long> accountToBidWonCount = new ConcurrentHashMap<>();
    private Map<Integer, Long> accountToNotificationEventCount = new ConcurrentHashMap<>();

    public RubiconAnalyticsModule(String endpointUrl, Integer globalSamplingFactor, String pbsVersion,
                                  String pbsHostname, String dataCenterRegion, BidderCatalog bidderCatalog,
                                  UidsCookieService uidsCookieService, UidsAuditCookieService uidsAuditCookieService,
                                  HttpClient httpClient, JacksonMapper mapper) {
        this.endpointUrl = HttpUtil.validateUrl(Objects.requireNonNull(endpointUrl) + EVENT_PATH);
        this.globalSamplingFactor = globalSamplingFactor;
        this.pbsVersion = pbsVersion;
        this.pbsHostname = Objects.requireNonNull(pbsHostname);
        this.dataCenterRegion = Objects.requireNonNull(dataCenterRegion);
        this.bidderCatalog = Objects.requireNonNull(bidderCatalog);
        this.uidsCookieService = Objects.requireNonNull(uidsCookieService);
        this.uidsAuditCookieService = Objects.requireNonNull(uidsAuditCookieService);
        this.httpClient = Objects.requireNonNull(httpClient);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public <T> void processEvent(T event) {
        if (event instanceof AuctionEvent) {
            processAuctionEvent((AuctionEvent) event);
        } else if (event instanceof AmpEvent) {
            processAmpEvent((AmpEvent) event);
        } else if (event instanceof NotificationEvent) {
            processNotificationEvent((NotificationEvent) event);
        }
    }

    @Override
    public Future<BidResponse> postProcess(RoutingContext context, UidsCookie uidsCookie, BidRequest bidRequest,
                                           BidResponse bidResponse, Account account) {
        if (bidRequest.getApp() != null) {
            final ExtBidResponse extBidResponse = readExt(bidResponse.getExt(), ExtBidResponse.class);
            final HttpContext httpContext = HttpContext.from(context);
            final boolean hasRubiconId = hasRubiconId(httpContext);

            final Integer accountId = parseId(account.getId());
            final Integer accountSamplingFactor = account.getAnalyticsSamplingFactor();

            for (final SeatBid seatBid : bidResponse.getSeatbid()) {
                final String bidder = seatBid.getSeat();
                final Integer responseTime = serverLatencyMillisFrom(extBidResponse, bidder);
                final Boolean serverHasUserId = serverHasUserIdFrom(uidsCookie, bidder);

                for (final Bid bid : seatBid.getBid()) {
                    // only continue if counter matches sampling factor
                    if (shouldProcessEvent(accountId, accountSamplingFactor, accountToBidWonCount, bidWonCount)) {
                        addEventCallbackToBid(bid,
                                toBidWonEvent(httpContext, bidRequest, bidder, responseTime, serverHasUserId,
                                        hasRubiconId, bid, accountId, accountSamplingFactor));
                    }
                }
            }
        }

        return Future.succeededFuture(bidResponse);
    }

    private boolean shouldProcessEvent(Integer accountId, Integer accountSamplingFactor,
                                       Map<Integer, Long> accountToEventCount, AtomicLong globalEventCount) {
        final boolean result;

        if (accountSamplingFactor != null && accountSamplingFactor > 0) {
            final long eventCount = accountToEventCount.compute(accountId,
                    (ignored, oldValue) -> oldValue == null ? 1L : oldValue + 1);
            result = eventCount % accountSamplingFactor == 0;
        } else if (globalSamplingFactor != null && globalSamplingFactor > 0) {
            result = globalEventCount.incrementAndGet() % globalSamplingFactor == 0;
        } else {
            result = false;
        }

        return result;
    }

    private void addEventCallbackToBid(Bid bid, Event event) {
        final byte[] eventBytes;
        try {
            eventBytes = mapper.mapper().writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            // not expected to happen though
            logger.warn("Exception occurred while marshalling analytics event", e);
            return;
        }

        final String encodedEvent = BASE64_ENCODER.encodeToString(eventBytes);
        final String url = String.format("%s?type=bidWon&data=%s", endpointUrl, encodedEvent);

        bid.setNurl(url);
    }

    private void processAuctionEvent(AuctionEvent auctionEvent) {
        final AuctionContext auctionContext = auctionEvent.getAuctionContext();
        if (auctionContext == null) { // this can happens when exception is thrown while processing
            return;
        }

        final HttpContext httpContext = auctionEvent.getHttpContext();
        final BidRequest bidRequest = auctionContext.getBidRequest();
        final Account account = auctionContext.getAccount();
        final BidResponse bidResponse = auctionEvent.getBidResponse();

        // only send event for mobile requests
        if (httpContext == null || bidRequest == null || account == null || bidResponse == null
                || bidRequest.getApp() == null) {
            return;
        }

        final Integer accountId = parseId(account.getId());
        final Integer accountSamplingFactor = account.getAnalyticsSamplingFactor();

        // only continue if counter matches sampling factor
        if (shouldProcessEvent(accountId, accountSamplingFactor, accountToAuctionEventCount, auctionEventCount)) {
            final UidsCookie uidsCookie = uidsCookieService.parseFromCookies(httpContext.getCookies());
            final List<AdUnit> adUnits = toAdUnits(bidRequest, uidsCookie, bidResponse);

            postEvent(toAuctionEvent(httpContext, bidRequest, adUnits, accountId, accountSamplingFactor,
                    this::eventBuilderBaseFromApp));
        }
    }

    private void processAmpEvent(AmpEvent ampEvent) {
        final AuctionContext auctionContext = ampEvent.getAuctionContext();
        if (auctionContext == null) { // this can happens when exception is thrown while processing
            return;
        }

        final HttpContext httpContext = ampEvent.getHttpContext();
        final BidRequest bidRequest = auctionContext.getBidRequest();
        final Account account = auctionContext.getAccount();
        final BidResponse bidResponse = ampEvent.getBidResponse();

        // only send event for web requests
        if (httpContext == null || bidRequest == null || account == null || bidResponse == null
                || bidRequest.getSite() == null) {
            return;
        }

        final String requestAccountId = account.getId();
        final Integer accountId = NumberUtils.isDigits(requestAccountId) ? parseId(requestAccountId) : null;
        final Integer accountSamplingFactor = account.getAnalyticsSamplingFactor();

        // only continue if counter matches sampling factor
        if (shouldProcessEvent(accountId, accountSamplingFactor, accountToAmpEventCount, ampEventCount)) {
            final UidsCookie uidsCookie = uidsCookieService.parseFromCookies(httpContext.getCookies());
            final List<AdUnit> adUnits = toAdUnits(bidRequest, uidsCookie, bidResponse);

            postEvent(toAuctionEvent(httpContext, bidRequest, adUnits, accountId, accountSamplingFactor,
                    this::eventBuilderBaseFromSite));
        }
    }

    private void processNotificationEvent(NotificationEvent notificationEvent) {
        final String bidId = notificationEvent.getBidId();
        final Account account = notificationEvent.getAccount();
        final HttpContext context = notificationEvent.getHttpContext();

        if (bidId == null || account == null || context == null) {
            return;
        }

        final Integer accountId = parseId(account.getId());
        final Integer accountSamplingFactor = account.getAnalyticsSamplingFactor();

        // only continue if counter matches sampling factor
        // note: this event type doesn't use global event count because it is related to particular account
        final Integer samplingFactor = samplingFactor(accountSamplingFactor);
        if (samplingFactor != null && samplingFactor > 0) {
            final long eventCount = accountToNotificationEventCount.compute(accountId,
                    (ignored, oldValue) -> oldValue == null ? 1L : oldValue + 1);
            if (eventCount % samplingFactor != 0) {
                return;
            }
        } else {
            return;
        }

        final NotificationEvent.Type type = notificationEvent.getType();
        final UidsCookie uidsCookie = uidsCookieService.parseFromCookies(context.getCookies());
        final Event event = type == NotificationEvent.Type.win
                ? makeWinEvent(bidId, accountId, context, uidsCookie)
                : makeImpEvent(bidId, accountId, context, uidsCookie);

        postEvent(event);
    }

    private Event makeWinEvent(String bidId, Integer accountId, HttpContext context,
                               UidsCookie uidsCookie) {
        return eventBuilderFromNotification(context)
                .bidsWon(Collections.singletonList(BidWon.builder()
                        .accountId(accountId)
                        .bidId(bidId)
                        .status(SUCCESS_STATUS)
                        .source(SERVER_SOURCE)
                        .serverHasUserId(serverHasUserIdFrom(uidsCookie, RUBICON_BIDDER))
                        .hasRubiconId(hasRubiconId(context))
                        .build()))
                .build();
    }

    private Event makeImpEvent(String bidId, Integer accountId, HttpContext context,
                               UidsCookie uidsCookie) {
        return eventBuilderFromNotification(context)
                .impressions(Collections.singletonList(Impression.builder()
                        .bidder(RUBICON_BIDDER)
                        .accountId(accountId)
                        .bidId(bidId)
                        .status(SUCCESS_STATUS)
                        .source(SERVER_SOURCE)
                        .serverHasUserId(serverHasUserIdFrom(uidsCookie, RUBICON_BIDDER))
                        .hasRubiconId(hasRubiconId(context))
                        .build()))
                .build();
    }

    private Event.EventBuilder eventBuilderFromNotification(HttpContext context) {
        return Event.builder()
                .eventTimeMillis(Instant.now().toEpochMilli())
                .integration(PBS_INTEGRATION)
                .version(pbsVersion)
                .referrerUri(context.getHeaders().get(REFERER_HEADER))
                .limitAdTracking(StringUtils.equals(context.getHeaders().get(DNT_HEADER), "1"))
                .userAgent(context.getHeaders().get(USER_AGENT_HEADER))
                .eventCreator(EventCreator.of(pbsHostname, dataCenterRegion));
    }

    private List<AdUnit> toAdUnits(BidRequest bidRequest, UidsCookie uidsCookie, BidResponse bidResponse) {
        final Map<String, List<TwinBids>> impIdToBids = toBidsByImpId(bidRequest, uidsCookie, bidResponse);

        return bidRequest.getImp().stream()
                .map(imp -> toAdUnit(bidRequest, imp, impIdToBids.getOrDefault(imp.getId(), Collections.emptyList())))
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
                final String impId = bid.getImpid();
                final Imp imp = findImpById(bidRequest.getImp(), impId);

                impIdToBids.computeIfAbsent(impId, key -> new ArrayList<>())
                        .add(toTwinBids(bidder, imp, bid, SUCCESS_STATUS, null, responseTime, serverHasUserId));
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
                if (Objects.equals(bidder, PREBID_EXT) || analyticsBidExists(impIdToBids, impId, bidder)) {
                    continue;
                }

                final BidError bidError = bidErrorFrom(extBidResponse, bidder, impId);
                final String status = bidError != null ? ERROR_STATUS : NO_BID_STATUS;

                final Integer responseTime = serverLatencyMillisFrom(extBidResponse, bidder);
                final Boolean serverHasUserId = serverHasUserIdFrom(uidsCookie, bidder);

                impIdToBids.computeIfAbsent(impId, key -> new ArrayList<>())
                        .add(toTwinBids(bidder, imp, null, status, bidError, responseTime, serverHasUserId));
            }
        }
    }

    /**
     * Determines {@link BidError} if possible or returns null.
     */
    private static BidError bidErrorFrom(ExtBidResponse extBidResponse, String bidder, String impId) {
        final Map<String, List<ExtBidderError>> bidderToErrors = extBidResponse.getErrors();
        final List<ExtBidderError> bidderErrors = bidderToErrors != null ? bidderToErrors.get(bidder) : null;
        if (bidderErrors != null) {
            for (ExtBidderError extBidderError : bidderErrors) {
                final Set<String> impIds = extBidderError.getImpIds();
                if (CollectionUtils.isNotEmpty(impIds) && impIds.contains(impId)) {
                    return bidErrorFrom(extBidderError);
                }
            }
        }
        return null;
    }

    private static BidError bidErrorFrom(ExtBidderError extBidderError) {
        final BidError result;

        final BidderError.Type errorType = BidderError.Type.getByCode(extBidderError.getCode());
        if (errorType != null) {
            if (errorType == BidderError.Type.timeout) {
                result = BidError.timeoutError(extBidderError.getMessage());
            } else {
                result = BidError.requestError(extBidderError.getMessage());
            }
        } else {
            result = null;
        }

        return result;
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
                ? uidsCookie.hasLiveUidFrom(bidderCatalog.usersyncerByName(bidder).getCookieFamilyName())
                : null;
    }

    private boolean hasRubiconId(HttpContext context) {
        return uidsCookieService.parseHostCookie(context.getCookies()) != null;
    }

    private static Imp findImpById(List<Imp> imps, String impId) {
        return imps.stream()
                .filter(imp -> Objects.equals(imp.getId(), impId))
                .findFirst()
                .orElse(null);
    }

    private TwinBids toTwinBids(String bidder, Imp imp, Bid bid, String status,
                                BidError bidError,
                                Integer serverLatencyMillis,
                                Boolean serverHasUserId) {

        final org.prebid.server.rubicon.analytics.proto.Bid analyticsBid =
                org.prebid.server.rubicon.analytics.proto.Bid.builder()
                        .bidId(getIfNotNull(bid, Bid::getId))
                        .bidder(bidder)
                        .status(status)
                        .error(bidError)
                        .source(SERVER_SOURCE)
                        .serverLatencyMillis(serverLatencyMillis)
                        .serverHasUserId(serverHasUserId)
                        .params(paramsFrom(imp, bidder))
                        .bidResponse(analyticsBidResponse(bid, mediaTypeString(mediaTypeFromBid(bid))))
                        .build();

        return new TwinBids(bid, analyticsBid);
    }

    private Params paramsFrom(Imp imp, String bidder) {
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

    private BidType mediaTypeFromBid(Bid bid) {
        final ExtPrebid<ExtBidPrebid, ObjectNode> extBid = bid != null ? readExtPrebid(bid.getExt()) : null;
        final ExtBidPrebid extBidPrebid = extBid != null ? extBid.getPrebid() : null;
        return extBidPrebid != null ? extBidPrebid.getType() : null;
    }

    private static String mediaTypeString(BidType bidType) {
        return bidType != null ? bidType.name() : null;
    }

    private <T> T readExt(ObjectNode ext, Class<T> type) {
        if (ext == null) {
            return null;
        }

        try {
            return mapper.mapper().treeToValue(ext, type);
        } catch (JsonProcessingException e) {
            logger.warn("Error unmarshalling ext by class {0}", e, type);
            return null;
        }
    }

    private ExtPrebid<ExtBidPrebid, ObjectNode> readExtPrebid(ObjectNode ext) {
        if (ext == null) {
            return null;
        }

        try {
            return mapper.mapper().convertValue(ext, EXT_PREBID_TYPE_REFERENCE);
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

    private AdUnit toAdUnit(BidRequest bidRequest, Imp imp, List<TwinBids> bids) {
        final boolean openrtbBidsFound = bids.stream().map(TwinBids::getOpenrtbBid).anyMatch(Objects::nonNull);

        final boolean errorsFound = bids.stream().map(TwinBids::getAnalyticsBid)
                .map(org.prebid.server.rubicon.analytics.proto.Bid::getError)
                .anyMatch(Objects::nonNull);

        return AdUnit.builder()
                .transactionId(transactionIdFrom(bidRequest.getId(), imp.getId()))
                .status(openrtbBidsFound ? SUCCESS_STATUS : errorsFound ? ERROR_STATUS : NO_BID_STATUS)
                .error(null) // multiple errors may exist, we do not have insight what to choose
                .mediaTypes(mediaTypesFromImp(imp))
                .videoAdFormat(imp.getVideo() != null ? videoAdFormatFromImp(imp, bids) : null)
                .dimensions(dimensions(imp))
                .adUnitCode(null) // does not apply to mobile ads
                .adserverTargeting(targetingForImp(bids))
                .bids(bids.stream().map(TwinBids::getAnalyticsBid).collect(Collectors.toList()))
                .build();
    }

    private static String transactionIdFrom(String bidRequestId, String impId) {
        return String.format("%s-%s", bidRequestId, impId);
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

    private String videoAdFormatFromImp(Imp imp, List<TwinBids> bids) {
        final boolean hasRubiconBid = bids.stream()
                .map(TwinBids::getAnalyticsBid)
                .anyMatch(RubiconAnalyticsModule::isRubiconVideoBid);

        return videoAdFormat(imp, hasRubiconBid);
    }

    private String videoAdFormatFromImp(Imp imp, String bidder) {
        return imp != null ? videoAdFormat(imp, Objects.equals(bidder, RUBICON_BIDDER)) : null;
    }

    private static boolean isRubiconVideoBid(org.prebid.server.rubicon.analytics.proto.Bid bid) {
        return Objects.equals(bid.getBidder(), RUBICON_BIDDER)
                && bid.getBidResponse() != null
                && Objects.equals(bid.getBidResponse().getMediaType(), BidType.video.name());
    }

    private String videoAdFormat(Imp imp, boolean hasRubiconBid) {
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

    private Map<String, String> targetingForImp(List<TwinBids> bids) {
        return bids.stream()
                .map(this::targetingFromBid)
                .filter(Objects::nonNull)
                .reduce((first, second) -> second)
                .orElse(null);
    }

    private Map<String, String> targetingFromBid(TwinBids bid) {
        final Bid openrtbBid = bid.getOpenrtbBid();
        final ExtPrebid<ExtBidPrebid, ObjectNode> extBid = openrtbBid != null
                ? readExtPrebid(openrtbBid.getExt())
                : null;
        return extBid != null ? extBid.getPrebid().getTargeting() : null;
    }

    private Event toAuctionEvent(HttpContext context, BidRequest bidRequest, List<AdUnit> adUnits, Integer accountId,
                                 Integer accountSamplingFactor,
                                 BiFunction<HttpContext, BidRequest, Event.EventBuilder> eventBuilderBase) {
        return eventBuilderBase.apply(context, bidRequest)
                .auctions(Collections.singletonList(Auction.of(
                        bidRequest.getId(),
                        samplingFactor(accountSamplingFactor),
                        adUnits,
                        accountId,
                        bidRequest.getTmax(),
                        hasRubiconId(context))))
                .build();
    }

    private Event toBidWonEvent(HttpContext context, BidRequest bidRequest, String bidder,
                                Integer serverLatencyMillis, Boolean serverHasUserId, boolean hasRubiconId,
                                Bid bid, Integer accountId, Integer accountSamplingFactor) {
        final Imp foundImp = findImpById(bidRequest.getImp(), bid.getImpid());
        final BidType bidType = mediaTypeFromBid(bid);
        final String bidTypeString = mediaTypeString(bidType);

        return eventBuilderBaseFromApp(context, bidRequest)
                .bidsWon(Collections.singletonList(BidWon.builder()
                        .transactionId(transactionIdFrom(bidRequest.getId(), bid.getImpid()))
                        .accountId(accountId)
                        .bidder(bidder)
                        .samplingFactor(samplingFactor(accountSamplingFactor))
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

    private static Integer parseId(String id) {
        try {
            return NumberUtils.createInteger(id);
        } catch (NumberFormatException e) {
            logger.warn("Id [{0}] is not a number", id);
            return null;
        }
    }

    private Integer samplingFactor(Integer accountSamplingFactor) {
        return accountSamplingFactor != null && accountSamplingFactor > 0
                ? accountSamplingFactor
                : globalSamplingFactor;
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
    private Event.EventBuilder eventBuilderBaseFromApp(HttpContext context, BidRequest bidRequest) {
        final App app = bidRequest.getApp();
        final ExtApp appExt = readExt(app.getExt(), ExtApp.class);
        final ExtAppPrebid appExtPrebid = appExt != null ? appExt.getPrebid() : null;

        return eventBuilderBase(context, bidRequest, org.prebid.server.rubicon.analytics.proto.App.of(
                app.getBundle(),
                app.getVer(),
                getIfNotNull(appExtPrebid, ExtAppPrebid::getVersion),
                getIfNotNull(appExtPrebid, ExtAppPrebid::getSource)));
    }

    /**
     * Prepares event from request from mobile web.
     */
    private Event.EventBuilder eventBuilderBaseFromSite(HttpContext context, BidRequest bidRequest) {
        return eventBuilderBase(context, bidRequest, null)
                .referrerUri(getIfNotNull(bidRequest.getSite(), Site::getPage));
    }

    /**
     * Prepares event from request.
     */
    private Event.EventBuilder eventBuilderBase(HttpContext context, BidRequest bidRequest,
                                                org.prebid.server.rubicon.analytics.proto.App clientApp) {
        final Device device = bidRequest.getDevice();
        final Integer deviceLmt = getIfNotNull(device, Device::getLmt);
        final ExtRequestPrebidBiddersRubicon extParameter = parseExtParameters(bidRequest);
        final String extIntegration = extParameter.getIntegration();
        final String extWrappername = extParameter.getWrappername();

        return Event.builder()
                .integration(StringUtils.isBlank(extIntegration) ? PBS_INTEGRATION : extIntegration)
                .wrappername(extWrappername)
                .version(pbsVersion)
                .client(Client.builder()
                        .deviceClass(MOBILE_DEVICE_CLASS)
                        .os(getIfNotNull(device, Device::getOs))
                        .osVersion(getIfNotNull(device, Device::getOsv))
                        .make(getIfNotNull(device, Device::getMake))
                        .model(getIfNotNull(device, Device::getModel))
                        .carrier(getIfNotNull(device, Device::getCarrier))
                        .connectionType(getIfNotNull(device, Device::getConnectiontype))
                        .app(clientApp)
                        .build())
                .limitAdTracking(deviceLmt != null ? deviceLmt != 0 : null)
                .eventCreator(EventCreator.of(pbsHostname, dataCenterRegion))
                .userAgent(getIfNotNull(device, Device::getUa))
                .country(ObjectUtils.defaultIfNull(countryFrom(device), countryFrom(context)));
    }

    private static <T, R> R getIfNotNull(T target, Function<T, R> getter) {
        return target != null ? getter.apply(target) : null;
    }

    private ExtRequestPrebidBiddersRubicon parseExtParameters(BidRequest bidRequest) {
        try {
            final ExtRequest extRequest = mapper.mapper().convertValue(bidRequest.getExt(), ExtRequest.class);
            final ExtRequestPrebid prebid = extRequest == null ? null : extRequest.getPrebid();
            final ExtRequestPrebidBidders bidders = prebid == null ? null : prebid.getBidders();
            return bidders == null ? ExtRequestPrebidBiddersRubicon.EMPTY : bidders.getRubicon();
        } catch (IllegalArgumentException e) {
            return ExtRequestPrebidBiddersRubicon.EMPTY;
        }
    }

    private String countryFrom(Device device) {
        final Geo geo = device != null ? device.getGeo() : null;
        return geo != null ? geo.getCountry() : null;
    }

    private String countryFrom(HttpContext context) {
        final UidAudit uidsAudit = uidsAuditCookieService.getUidsAudit(context.getCookies());
        return uidsAudit != null ? uidsAudit.getCountry() : null;
    }

    /**
     * Sends event to analytics service.
     */
    private void postEvent(Event event) {
        httpClient.post(endpointUrl, headers(event), mapper.encode(event), 2000L)
                .compose(RubiconAnalyticsModule::processResponse)
                .recover(RubiconAnalyticsModule::failResponse);
    }

    /**
     * Returns headers needed for analytic request.
     */
    private static MultiMap headers(Event event) {
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap()
                .add(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);

        final String userAgent = event.getUserAgent();
        if (userAgent != null) {
            headers.add(HttpHeaders.USER_AGENT, userAgent);
        }
        return headers;
    }

    /**
     * Handles {@link HttpClientResponse}, analyzes response status
     * and creates {@link Future} of {@link Void} from body content
     * or throws {@link PreBidException} in case of errors.
     */
    private static Future<Void> processResponse(HttpClientResponse response) {
        final int statusCode = response.getStatusCode();
        if (statusCode < 200 || statusCode > 299) {
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
