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
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtBidRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtImpPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestCurrency;
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
import org.prebid.server.rubicon.analytics.proto.Event;
import org.prebid.server.rubicon.analytics.proto.EventCreator;
import org.prebid.server.rubicon.analytics.proto.ExtApp;
import org.prebid.server.rubicon.analytics.proto.ExtAppPrebid;
import org.prebid.server.rubicon.analytics.proto.Impression;
import org.prebid.server.rubicon.analytics.proto.Params;
import org.prebid.server.rubicon.analytics.proto.StartDelay;
import org.prebid.server.rubicon.analytics.proto.VideoAdFormat;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.rubicon.audit.proto.UidAudit;
import org.prebid.server.rubicon.proto.request.ExtRequest;
import org.prebid.server.rubicon.proto.request.ExtRequestPrebid;
import org.prebid.server.rubicon.proto.request.ExtRequestPrebidBidders;
import org.prebid.server.rubicon.proto.request.ExtRequestPrebidBiddersRubicon;
import org.prebid.server.settings.model.Account;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.vertx.http.HttpClient;
import org.prebid.server.vertx.http.model.HttpClientResponse;

import java.math.BigDecimal;
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

    private static final String STORED_REQUEST_ID_AMP_URL_PARAM = "tag_id=";
    private static final String URL_PARAM_SEPARATOR = "&";

    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private static final Map<Integer, String> VIDEO_SIZE_AD_FORMATS;

    private static final String USD_CURRENCY = "USD";

    private static final TypeReference<ExtPrebid<ExtBidPrebid, ObjectNode>> EXT_PREBID_TYPE_REFERENCE =
            new TypeReference<ExtPrebid<ExtBidPrebid, ObjectNode>>() {
            };

    private static final TypeReference<ExtPrebid<ExtImpPrebid, ObjectNode>> IMP_EXT_TYPE_REFERENCE =
            new TypeReference<ExtPrebid<ExtImpPrebid, ObjectNode>>() {
            };
    private static final String APPLICATION_JSON =
            HttpHeaderValues.APPLICATION_JSON.toString() + ";" + HttpHeaderValues.CHARSET.toString() + "=" + "utf-8";

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
    private final String pbsVersion;
    private final Integer globalSamplingFactor;
    private final String pbsHostname;
    private final String dataCenterRegion;
    private final BidderCatalog bidderCatalog;
    private final UidsCookieService uidsCookieService;
    private final UidsAuditCookieService uidsAuditCookieService;
    private final CurrencyConversionService currencyService;
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
                                  CurrencyConversionService currencyService, HttpClient httpClient,
                                  JacksonMapper mapper) {
        this.endpointUrl = HttpUtil.validateUrl(Objects.requireNonNull(endpointUrl) + EVENT_PATH);
        this.globalSamplingFactor = globalSamplingFactor;
        this.pbsVersion = pbsVersion;
        this.pbsHostname = Objects.requireNonNull(pbsHostname);
        this.dataCenterRegion = Objects.requireNonNull(dataCenterRegion);
        this.bidderCatalog = Objects.requireNonNull(bidderCatalog);
        this.uidsCookieService = Objects.requireNonNull(uidsCookieService);
        this.uidsAuditCookieService = Objects.requireNonNull(uidsAuditCookieService);
        this.currencyService = Objects.requireNonNull(currencyService);
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
    public Future<BidResponse> postProcess(RoutingContext routingContext, UidsCookie uidsCookie, BidRequest bidRequest,
                                           BidResponse bidResponse, Account account) {
        if (bidRequest.getApp() != null) {
            final ExtBidResponse extBidResponse = readExt(bidResponse.getExt(), ExtBidResponse.class);
            final HttpContext httpContext = HttpContext.from(routingContext);
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
                    this::eventBuilderBaseFromApp), isDebugEnabled(bidRequest));
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
        final String storedId = parseStoredId(httpContext.getUri());

        // only continue if counter matches sampling factor
        if (shouldProcessEvent(accountId, accountSamplingFactor, accountToAmpEventCount, ampEventCount)) {
            final UidsCookie uidsCookie = uidsCookieService.parseFromCookies(httpContext.getCookies());
            final List<AdUnit> adUnits = toAdUnits(bidRequest, uidsCookie, bidResponse, storedId);

            postEvent(toAuctionEvent(httpContext, bidRequest, adUnits, accountId, accountSamplingFactor,
                    this::eventBuilderBaseFromSite), isDebugEnabled(bidRequest));
        }
    }

    private String parseStoredId(String uri) {
        // substringBetween can't handle "amp?tag_id=1001"
        final String tagIdValueAndOthers = StringUtils.substringAfter(uri, STORED_REQUEST_ID_AMP_URL_PARAM);
        return StringUtils.substringBefore(tagIdValueAndOthers, URL_PARAM_SEPARATOR);
    }

    private void processNotificationEvent(NotificationEvent notificationEvent) {
        final String bidId = notificationEvent.getBidId();
        final Account account = notificationEvent.getAccount();
        final HttpContext httpContext = notificationEvent.getHttpContext();
        if (bidId == null || account == null || httpContext == null) {
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

        final String bidder = notificationEvent.getBidder();
        final UidsCookie uidsCookie = uidsCookieService.parseFromCookies(httpContext.getCookies());
        final Long timestamp = notificationEvent.getTimestamp();
        final String integration = notificationEvent.getIntegration();

        final NotificationEvent.Type type = notificationEvent.getType();
        final Event event = type == NotificationEvent.Type.win
                ? makeWinEvent(bidId, bidder, accountId, httpContext, uidsCookie, timestamp, integration)
                : makeImpEvent(bidId, bidder, accountId, httpContext, uidsCookie, timestamp, integration);

        postEvent(event, false);
    }

    private Event makeWinEvent(String bidId, String bidder, Integer accountId, HttpContext httpContext,
                               UidsCookie uidsCookie, Long timestamp, String integration) {
        return eventBuilderFromNotification(httpContext, timestamp, integration)
                .bidsWon(Collections.singletonList(BidWon.builder()
                        .bidId(bidId)
                        .bidder(bidder)
                        .accountId(accountId)
                        .status(SUCCESS_STATUS)
                        .source(SERVER_SOURCE)
                        .serverHasUserId(serverHasUserIdFrom(uidsCookie, RUBICON_BIDDER))
                        .hasRubiconId(hasRubiconId(httpContext))
                        .build()))
                .build();
    }

    private Event makeImpEvent(String bidId, String bidder, Integer accountId, HttpContext httpContext,
                               UidsCookie uidsCookie,
                               Long timestamp, String integration) {
        return eventBuilderFromNotification(httpContext, timestamp, integration)
                .impressions(Collections.singletonList(Impression.builder()
                        .bidId(bidId)
                        .bidder(bidder)
                        .accountId(accountId)
                        .status(SUCCESS_STATUS)
                        .source(SERVER_SOURCE)
                        .serverHasUserId(serverHasUserIdFrom(uidsCookie, RUBICON_BIDDER))
                        .hasRubiconId(hasRubiconId(httpContext))
                        .build()))
                .build();
    }

    private Event.EventBuilder eventBuilderFromNotification(HttpContext httpContext, Long timestamp,
                                                            String integration) {
        return Event.builder()
                .eventTimeMillis(timestamp != null ? timestamp : Instant.now().toEpochMilli())
                .integration(ObjectUtils.defaultIfNull(integration, PBS_INTEGRATION))
                .version(pbsVersion)
                .referrerUri(httpContext.getHeaders().get(REFERER_HEADER))
                .limitAdTracking(StringUtils.equals(httpContext.getHeaders().get(DNT_HEADER), "1"))
                .userAgent(httpContext.getHeaders().get(USER_AGENT_HEADER))
                .eventCreator(EventCreator.of(pbsHostname, dataCenterRegion));
    }

    private boolean isDebugEnabled(BidRequest bidRequest) {
        final ObjectNode ext = bidRequest.getExt();
        final ExtBidRequest extBidRequest = ext != null ? readExt(ext, ExtBidRequest.class) : null;
        final org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid extRequestPrebid = extBidRequest != null
                ? extBidRequest.getPrebid()
                : null;
        final Integer prebidDebug = extRequestPrebid != null ? extRequestPrebid.getDebug() : null;
        return Objects.equals(prebidDebug, 1);
    }

    private List<AdUnit> toAdUnits(BidRequest bidRequest,
                                   UidsCookie uidsCookie,
                                   BidResponse bidResponse,
                                   String storedId) {

        final Map<String, List<TwinBids>> impIdToBids = toBidsByImpId(bidRequest, uidsCookie, bidResponse);

        return bidRequest.getImp().stream()
                .map(imp -> toAdUnit(bidRequest, imp, impIdToBids.getOrDefault(imp.getId(), Collections.emptyList()),
                        storedId))
                .collect(Collectors.toList());
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

        final String currency = bidRequest.getCur().get(0);
        final Map<String, Map<String, BigDecimal>> requestCurrencyRates = requestCurrencyRates(bidRequest.getExt());

        for (final SeatBid seatBid : bidResponse.getSeatbid()) {
            final String bidder = seatBid.getSeat();
            final Integer responseTime = serverLatencyMillisFrom(extBidResponse, bidder);
            final Boolean serverHasUserId = serverHasUserIdFrom(uidsCookie, bidder);

            for (final Bid bid : seatBid.getBid()) {
                final String impId = bid.getImpid();
                final Imp imp = findImpById(bidRequest.getImp(), impId);

                impIdToBids.computeIfAbsent(impId, key -> new ArrayList<>())
                        .add(toTwinBids(bidder, imp, bid, SUCCESS_STATUS, null, responseTime, serverHasUserId,
                                currency, requestCurrencyRates));
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
                        .add(toTwinBids(bidder, imp, null, status, bidError, responseTime, serverHasUserId, null,
                                null));
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

    private boolean hasRubiconId(HttpContext httpContext) {
        return uidsCookieService.parseHostCookie(httpContext.getCookies()) != null;
    }

    private static Imp findImpById(List<Imp> imps, String impId) {
        return imps.stream()
                .filter(imp -> Objects.equals(imp.getId(), impId))
                .findFirst()
                .orElse(null);
    }

    private Map<String, Map<String, BigDecimal>> requestCurrencyRates(ObjectNode extBidRequestNode) {
        final ExtBidRequest extBidRequest = extBidRequestNode != null
                ? readExt(extBidRequestNode, ExtBidRequest.class)
                : null;

        final org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid prebid = extBidRequest != null
                ? extBidRequest.getPrebid()
                : null;

        final ExtRequestCurrency currency = prebid != null ? prebid.getCurrency() : null;
        return currency != null ? currency.getRates() : null;
    }

    private TwinBids toTwinBids(String bidder, Imp imp, Bid bid, String status,
                                BidError bidError,
                                Integer serverLatencyMillis,
                                Boolean serverHasUserId,
                                String currency,
                                Map<String, Map<String, BigDecimal>> requestCurrencyRates) {

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
                        .bidResponse(analyticsBidResponse(bid, mediaTypeString(mediaTypeFromBid(bid)), currency,
                                requestCurrencyRates))
                        .build();

        return new TwinBids(bid, analyticsBid);
    }

    private Params paramsFrom(Imp imp, String bidder) {
        if (imp != null && Objects.equals(bidder, RUBICON_BIDDER)) {
            // it should be safe to cast since there wouldn't be rubicon bids if this imp had no "rubicon" field in ext
            final ExtImpRubicon impExt = readExt((ObjectNode) imp.getExt().get(RUBICON_BIDDER), ExtImpRubicon.class);

            return impExt != null ? Params.of(impExt.getAccountId(), impExt.getSiteId(), impExt.getZoneId()) : null;
        }
        return null;
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

    private org.prebid.server.rubicon.analytics.proto.BidResponse analyticsBidResponse(
            Bid bid, String mediaType, String currency, Map<String, Map<String, BigDecimal>> requestCurrencyRates) {

        return bid != null
                ? org.prebid.server.rubicon.analytics.proto.BidResponse.of(
                parseId(bid.getDealid()),
                convertToUSD(bid.getPrice(), currency, requestCurrencyRates),
                mediaType,
                Dimensions.of(bid.getW(), bid.getH()))
                : null;
    }

    private BigDecimal convertToUSD(
            BigDecimal price, String currency, Map<String, Map<String, BigDecimal>> requestCurrencyRates) {

        try {
            return currencyService.convertCurrency(price, requestCurrencyRates, USD_CURRENCY, currency);
        } catch (PreBidException e) {
            logger.info("Unable to covert bid currency {0} to desired ad server currency {1}. {2}",
                    currency, USD_CURRENCY, e.getMessage());
            return null;
        }
    }

    private AdUnit toAdUnit(BidRequest bidRequest, Imp imp, List<TwinBids> bids) {
        final ExtPrebid<ExtImpPrebid, ExtImpRubicon> extPrebid = extPrebidFromImp(imp);
        final Params params = paramsFromPrebid(extPrebid.getBidder());
        final String storedImpId = storedRequestId(extPrebid.getPrebid());

        return toAdUnit(bidRequest, imp, bids, params, storedImpId);

    }

    private AdUnit toAdUnit(BidRequest bidRequest, Imp imp, List<TwinBids> bids, String storedImpId) {
        final ExtPrebid<ExtImpPrebid, ExtImpRubicon> extPrebid = extPrebidFromImp(imp);
        final Params params = paramsFromPrebid(extPrebid.getBidder());

        return toAdUnit(bidRequest, imp, bids, params, storedImpId);
    }

    private AdUnit toAdUnit(BidRequest bidRequest, Imp imp, List<TwinBids> bids, Params params, String storedId) {
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
                .adUnitCode(storedId)
                .siteId(params.getSiteId())
                .zoneId(params.getZoneId())
                .adserverTargeting(targetingForImp(bids))
                .bids(bids.stream().map(TwinBids::getAnalyticsBid).collect(Collectors.toList()))
                .build();
    }

    private ExtPrebid<ExtImpPrebid, ExtImpRubicon> extPrebidFromImp(Imp imp) {
        final ObjectNode impExt = imp != null ? imp.getExt() : null;
        if (impExt == null) {
            return ExtPrebid.of(null, null);
        }
        try {
            final ExtImpPrebid prebid =
                    mapper.mapper().convertValue(impExt, IMP_EXT_TYPE_REFERENCE).getPrebid();

            final ObjectNode impExtRubicon = (ObjectNode) impExt.get(RUBICON_BIDDER);
            final ExtImpRubicon impRubicon = impExtRubicon == null ? null : readExt(impExtRubicon, ExtImpRubicon.class);
            return ExtPrebid.of(prebid, impRubicon);
        } catch (IllegalArgumentException e) {
            logger.warn("Error unmarshalling ext by type reference {0}", e, IMP_EXT_TYPE_REFERENCE);
            return ExtPrebid.of(null, null);
        }
    }

    private Params paramsFromPrebid(ExtImpRubicon impExt) {
        return impExt != null
                ? Params.of(impExt.getAccountId(), impExt.getSiteId(), impExt.getZoneId())
                : Params.empty();
    }

    private String storedRequestId(ExtImpPrebid impPrebid) {
        final ExtStoredRequest storedRequest = impPrebid != null ? impPrebid.getStoredrequest() : null;
        return storedRequest != null ? storedRequest.getId() : null;
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
        if (hasRubiconBid) {
            // it should be safe to cast since there wouldn't be rubicon bids if this imp had no "rubicon" field in ext
            final ExtImpRubicon impExt = readExt((ObjectNode) imp.getExt().get(RUBICON_BIDDER), ExtImpRubicon.class);
            final RubiconVideoParams videoParams = impExt != null ? impExt.getVideo() : null;
            if (videoParams != null) {
                return VIDEO_SIZE_AD_FORMATS.get(videoParams.getSizeId());
            }
        }

        final Integer startDelay = imp.getVideo().getStartdelay();
        if (startDelay != null) {
            if (startDelay == StartDelay.GENERIC_MIDROLL || startDelay > StartDelay.PREROLL) {
                return VideoAdFormat.MIDROLL;
            } else if (startDelay == StartDelay.GENERIC_POSTROLL) {
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

    private Event toAuctionEvent(HttpContext httpContext, BidRequest bidRequest, List<AdUnit> adUnits,
                                 Integer accountId, Integer accountSamplingFactor,
                                 BiFunction<HttpContext, BidRequest, Event.EventBuilder> eventBuilderBase) {
        return eventBuilderBase.apply(httpContext, bidRequest)
                .auctions(Collections.singletonList(Auction.of(
                        bidRequest.getId(),
                        samplingFactor(accountSamplingFactor),
                        adUnits,
                        accountId,
                        bidRequest.getTmax(),
                        hasRubiconId(httpContext))))
                .build();
    }

    private Event toBidWonEvent(HttpContext httpContext, BidRequest bidRequest, String bidder,
                                Integer serverLatencyMillis, Boolean serverHasUserId, boolean hasRubiconId,
                                Bid bid, Integer accountId, Integer accountSamplingFactor) {
        final Imp foundImp = findImpById(bidRequest.getImp(), bid.getImpid());
        final BidType bidType = mediaTypeFromBid(bid);
        final String bidTypeString = mediaTypeString(bidType);

        return eventBuilderBaseFromApp(httpContext, bidRequest)
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
                        .bidResponse(analyticsBidResponse(bid, bidTypeString, bidRequest.getCur().get(0),
                                requestCurrencyRates(bidRequest.getExt())))
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
    private Event.EventBuilder eventBuilderBaseFromApp(HttpContext httpContext, BidRequest bidRequest) {
        final App app = bidRequest.getApp();
        final ExtApp appExt = readExt(app.getExt(), ExtApp.class);
        final ExtAppPrebid appExtPrebid = appExt != null ? appExt.getPrebid() : null;

        return eventBuilderBase(httpContext, bidRequest, org.prebid.server.rubicon.analytics.proto.App.of(
                app.getBundle(),
                app.getVer(),
                getIfNotNull(appExtPrebid, ExtAppPrebid::getVersion),
                getIfNotNull(appExtPrebid, ExtAppPrebid::getSource)));
    }

    /**
     * Prepares event from request from mobile web.
     */
    private Event.EventBuilder eventBuilderBaseFromSite(HttpContext httpContext, BidRequest bidRequest) {
        return eventBuilderBase(httpContext, bidRequest, null)
                .referrerUri(getIfNotNull(bidRequest.getSite(), Site::getPage));
    }

    /**
     * Prepares event from request.
     */
    private Event.EventBuilder eventBuilderBase(HttpContext httpContext, BidRequest bidRequest,
                                                org.prebid.server.rubicon.analytics.proto.App clientApp) {
        final Device device = bidRequest.getDevice();
        final Integer deviceLmt = getIfNotNull(device, Device::getLmt);
        final ExtRequestPrebidBiddersRubicon extParameter = parseExtParameters(bidRequest);
        final String extIntegration = extParameter.getIntegration();
        final String extWrappername = extParameter.getWrappername();

        return Event.builder()
                .integration(StringUtils.isBlank(extIntegration) ? PBS_INTEGRATION : extIntegration)
                .wrapperName(extWrappername)
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
                .country(ObjectUtils.defaultIfNull(countryFrom(device), countryFrom(httpContext)));
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

    private String countryFrom(HttpContext httpContext) {
        final UidAudit uidsAudit = uidsAuditCookieService.getUidsAudit(httpContext.getCookies());
        return uidsAudit != null ? uidsAudit.getCountry() : null;
    }

    /**
     * Sends event to analytics service.
     */
    private void postEvent(Event event, boolean isDebugEnabled) {
        final String eventBody = mapper.encode(event);
        if (isDebugEnabled) {
            logger.warn(String.format("Sending analytic event: %s", eventBody));
        }
        httpClient.post(endpointUrl, headers(event), eventBody, 2000L)
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
