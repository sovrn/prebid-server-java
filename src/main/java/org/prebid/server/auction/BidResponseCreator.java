package org.prebid.server.auction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.DataObject;
import com.iab.openrtb.request.Deal;
import com.iab.openrtb.request.ImageObject;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Native;
import com.iab.openrtb.request.Pmp;
import com.iab.openrtb.request.Request;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Asset;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.Response;
import com.iab.openrtb.response.SeatBid;
import io.vertx.core.Future;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Factory;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.auction.model.AuctionContext;
import org.prebid.server.auction.model.BidRequestCacheInfo;
import org.prebid.server.auction.model.BidderResponse;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.BidderSeatBid;
import org.prebid.server.cache.CacheService;
import org.prebid.server.cache.model.CacheContext;
import org.prebid.server.cache.model.CacheIdInfo;
import org.prebid.server.cache.model.CacheServiceResult;
import org.prebid.server.cache.model.DebugHttpCall;
import org.prebid.server.deals.model.DeepDebugLog;
import org.prebid.server.deals.model.TxnLog;
import org.prebid.server.events.EventsContext;
import org.prebid.server.events.EventsService;
import org.prebid.server.exception.InvalidRequestException;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.json.DecodeException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtImp;
import org.prebid.server.proto.openrtb.ext.request.ExtImpPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtMediaTypePriceGranularity;
import org.prebid.server.proto.openrtb.ext.request.ExtOptions;
import org.prebid.server.proto.openrtb.ext.request.ExtPriceGranularity;
import org.prebid.server.proto.openrtb.ext.request.ExtRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidChannel;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestTargeting;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.CacheAsset;
import org.prebid.server.proto.openrtb.ext.response.Events;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebidVideo;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponse;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponsePrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidderError;
import org.prebid.server.proto.openrtb.ext.response.ExtDebugPgmetrics;
import org.prebid.server.proto.openrtb.ext.response.ExtDebugTrace;
import org.prebid.server.proto.openrtb.ext.response.ExtHttpCall;
import org.prebid.server.proto.openrtb.ext.response.ExtResponseCache;
import org.prebid.server.proto.openrtb.ext.response.ExtResponseDebug;
import org.prebid.server.proto.openrtb.ext.response.ExtTraceDeal;
import org.prebid.server.settings.model.Account;
import org.prebid.server.settings.model.AccountAnalyticsConfig;
import org.prebid.server.settings.model.VideoStoredDataResult;
import org.prebid.server.util.LineItemUtil;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class BidResponseCreator {

    private static final TypeReference<ExtPrebid<ExtBidPrebid, ObjectNode>> EXT_PREBID_TYPE_REFERENCE =
            new TypeReference<ExtPrebid<ExtBidPrebid, ObjectNode>>() {
            };

    private static final String CACHE = "cache";
    private static final String PREBID_EXT = "prebid";

    private final CacheService cacheService;
    private final BidderCatalog bidderCatalog;
    private final EventsService eventsService;
    private final StoredRequestProcessor storedRequestProcessor;
    private final BidResponseReducer bidResponseReducer;
    private final boolean generateBidId;
    private final int truncateAttrChars;
    private final boolean enforceRandomBidId;
    private final Clock clock;
    private final JacksonMapper mapper;

    private final String cacheHost;
    private final String cachePath;
    private final String cacheAssetUrlTemplate;

    public BidResponseCreator(CacheService cacheService,
                              BidderCatalog bidderCatalog,
                              EventsService eventsService,
                              StoredRequestProcessor storedRequestProcessor,
                              BidResponseReducer bidResponseReducer,
                              boolean generateBidId,
                              int truncateAttrChars,
                              boolean enforceRandomBidId,
                              Clock clock,
                              JacksonMapper mapper) {

        this.cacheService = Objects.requireNonNull(cacheService);
        this.bidderCatalog = Objects.requireNonNull(bidderCatalog);
        this.eventsService = Objects.requireNonNull(eventsService);
        this.storedRequestProcessor = Objects.requireNonNull(storedRequestProcessor);
        this.bidResponseReducer = Objects.requireNonNull(bidResponseReducer);
        this.generateBidId = generateBidId;
        this.truncateAttrChars = validateTruncateAttrChars(truncateAttrChars);
        this.enforceRandomBidId = enforceRandomBidId;
        this.clock = Objects.requireNonNull(clock);
        this.mapper = Objects.requireNonNull(mapper);

        cacheHost = Objects.requireNonNull(cacheService.getEndpointHost());
        cachePath = Objects.requireNonNull(cacheService.getEndpointPath());
        cacheAssetUrlTemplate = Objects.requireNonNull(cacheService.getCachedAssetURLTemplate());
    }

    /**
     * Creates an OpenRTB {@link BidResponse} from the bids supplied by the bidder,
     * including processing of winning bids with cache IDs.
     */
    Future<BidResponse> create(List<BidderResponse> bidderResponses,
                               AuctionContext auctionContext,
                               BidRequestCacheInfo cacheInfo,
                               boolean debugEnabled) {

        final long auctionTimestamp = auctionTimestamp(auctionContext);

        if (isEmptyBidderResponses(bidderResponses)) {
            final BidRequest bidRequest = auctionContext.getBidRequest();
            return Future.succeededFuture(BidResponse.builder()
                    .id(bidRequest.getId())
                    .cur(bidRequest.getCur().get(0))
                    .nbr(0) // signal "Unknown Error"
                    .seatbid(Collections.emptyList())
                    .ext(mapper.mapper().valueToTree(toExtBidResponse(
                            bidderResponses,
                            auctionContext,
                            CacheServiceResult.empty(),
                            VideoStoredDataResult.empty(),
                            auctionTimestamp,
                            debugEnabled,
                            null)))
                    .build());
        }

        return cacheBidsAndCreateResponse(
                bidderResponses,
                auctionContext,
                cacheInfo,
                auctionTimestamp,
                debugEnabled);
    }

    private static int validateTruncateAttrChars(int truncateAttrChars) {
        if (truncateAttrChars < 0 || truncateAttrChars > 255) {
            throw new IllegalArgumentException("truncateAttrChars must be between 0 and 255");
        }

        return truncateAttrChars;
    }

    /**
     * Checks whether bidder responses are empty or contain no bids.
     */
    private static boolean isEmptyBidderResponses(List<BidderResponse> bidderResponses) {
        return bidderResponses.isEmpty() || bidderResponses.stream()
                .map(bidderResponse -> bidderResponse.getSeatBid().getBids())
                .allMatch(CollectionUtils::isEmpty);
    }

    private Future<BidResponse> cacheBidsAndCreateResponse(List<BidderResponse> bidderResponses,
                                                           AuctionContext auctionContext,
                                                           BidRequestCacheInfo cacheInfo,
                                                           long auctionTimestamp,
                                                           boolean debugEnabled) {

        final BidRequest bidRequest = auctionContext.getBidRequest();

        final List<Imp> imps = bidRequest.getImp();
        final List<BidderResponse> reducedBidderResponses = bidderResponses.stream()
                .map(bidderResponse -> bidResponseReducer.removeRedundantBids(bidderResponse, imps))
                .collect(Collectors.toList());

        final List<BidderResponse> updatedBidderResponses = checkAndGenerateBidIds(reducedBidderResponses);

        ExtRequestTargeting targeting = targeting(bidRequest);
        final Set<Bid> winningBids = newOrEmptySet(targeting);
        final Set<Bid> winningBidsByBidder = newOrEmptySet(targeting);

        final TxnLog txnLog = auctionContext.getTxnLog();

        // determine winning bids only if targeting is present
        if (targeting != null) {
            populateWinningBids(updatedBidderResponses, winningBids, winningBidsByBidder, imps, txnLog);
        }

        if (!winningBids.isEmpty()) {
            winningBids.stream()
                    .map(bid -> LineItemUtil.lineItemIdFrom(bid, imps, mapper))
                    .filter(Objects::nonNull)
                    .forEach(lineItemId ->
                            auctionContext.getTxnLog().lineItemSentToClientAsTopMatch().add(lineItemId));
        }

        updateSentToClientTxnLog(updatedBidderResponses, auctionContext, imps);

        final Set<Bid> bidsToCache = cacheInfo.isShouldCacheWinningBidsOnly()
                ? winningBids
                : updatedBidderResponses.stream().flatMap(BidResponseCreator::getBids).collect(Collectors.toSet());

        final EventsContext eventsContext = EventsContext.builder()
                .enabledForAccount(eventsEnabledForAccount(auctionContext))
                .enabledForRequest(eventsEnabledForRequest(auctionContext))
                .auctionTimestamp(auctionTimestamp)
                .integration(integrationFrom(auctionContext))
                .build();

        return toBidsWithCacheIds(
                updatedBidderResponses,
                bidsToCache,
                auctionContext,
                cacheInfo,
                eventsContext)
                .compose(cacheResult -> videoStoredDataResult(bidRequest.getImp(), auctionContext.getTimeout())
                        .map(videoStoredDataResult -> toBidResponse(
                                updatedBidderResponses,
                                auctionContext,
                                targeting,
                                winningBids,
                                winningBidsByBidder,
                                cacheInfo,
                                cacheResult,
                                videoStoredDataResult,
                                eventsContext,
                                auctionTimestamp,
                                debugEnabled,
                                imps)));
    }

    private static ExtRequestTargeting targeting(BidRequest bidRequest) {
        final ExtRequest requestExt = bidRequest.getExt();
        final ExtRequestPrebid prebid = requestExt != null ? requestExt.getPrebid() : null;
        return prebid != null ? prebid.getTargeting() : null;
    }

    /**
     * Extracts auction timestamp from {@link ExtRequest} or get it from {@link Clock} if it is null.
     */
    private long auctionTimestamp(AuctionContext auctionContext) {
        final ExtRequest requestExt = auctionContext.getBidRequest().getExt();
        final ExtRequestPrebid prebid = requestExt != null ? requestExt.getPrebid() : null;
        final Long auctionTimestamp = prebid != null ? prebid.getAuctiontimestamp() : null;
        return auctionTimestamp != null ? auctionTimestamp : clock.millis();
    }

    /**
     * Increments sent to client metrics for each bid with deal.
     */
    private void updateSentToClientTxnLog(List<BidderResponse> bidderResponses, AuctionContext auctionContext,
                                          List<Imp> imps) {
        bidderResponses.stream()
                .map(BidderResponse::getSeatBid)
                .map(BidderSeatBid::getBids)
                .flatMap(Collection::stream)
                .map(BidderBid::getBid)
                .map(bid -> LineItemUtil.lineItemIdFrom(bid, imps, mapper))
                .filter(Objects::nonNull)
                .forEach(lineItemId -> auctionContext.getTxnLog().lineItemsSentToClient().add(lineItemId));
    }

    /**
     * Returns {@link ExtBidResponse} object, populated with response time, errors and debug info (if requested)
     * from all bidders.
     */
    private ExtBidResponse toExtBidResponse(List<BidderResponse> bidderResponses,
                                            AuctionContext auctionContext,
                                            CacheServiceResult cacheResult,
                                            VideoStoredDataResult videoStoredDataResult,
                                            long auctionTimestamp,
                                            boolean debugEnabled,
                                            Map<String, List<ExtBidderError>> bidErrors) {

        final BidRequest bidRequest = auctionContext.getBidRequest();

        final ExtResponseDebug extResponseDebug =
                toExtResponseDebug(bidderResponses, auctionContext, cacheResult, debugEnabled);
        final Map<String, List<ExtBidderError>> errors =
                toExtBidderErrors(bidderResponses, auctionContext, cacheResult, videoStoredDataResult, bidErrors);
        final Map<String, Integer> responseTimeMillis = toResponseTimes(bidderResponses, cacheResult);

        return ExtBidResponse.of(extResponseDebug, errors, responseTimeMillis, bidRequest.getTmax(), null,
                ExtBidResponsePrebid.of(auctionTimestamp));
    }

    /**
     * If enforceRandomBidId is set, then bid adapters that return seatbid[].bid[].id less than 17 characters
     * will have the bid.id overwritten with a decent ~40-char UUID.
     */
    private List<BidderResponse> checkAndGenerateBidIds(List<BidderResponse> bidderResponses) {
        if (enforceRandomBidId) {
            bidderResponses.forEach(BidResponseCreator::checkAndGenerateBidIds);
        }
        return bidderResponses;
    }

    private static void checkAndGenerateBidIds(BidderResponse bidderResponse) {
        bidderResponse.getSeatBid().getBids().forEach(BidResponseCreator::checkAndGenerateBidId);
    }

    private static void checkAndGenerateBidId(BidderBid bidderBid) {
        final Bid bid = bidderBid.getBid();
        if (StringUtils.length(bid.getId()) < 17) {
            bid.setId(UUID.randomUUID().toString());
        }
    }

    private ExtResponseDebug toExtResponseDebug(List<BidderResponse> bidderResponses, AuctionContext auctionContext,
                                                CacheServiceResult cacheResult, boolean debugEnabled) {
        final DeepDebugLog deepDebugLog = auctionContext.getDeepDebugLog();

        final Map<String, List<ExtHttpCall>> httpCalls = debugEnabled
                ? toExtHttpCalls(bidderResponses, cacheResult, auctionContext.getDebugHttpCalls())
                : null;
        final BidRequest bidRequest = debugEnabled ? auctionContext.getBidRequest() : null;
        final ExtDebugPgmetrics extDebugPgmetrics = debugEnabled ? toExtDebugPgmetrics(
                auctionContext.getTxnLog()) : null;
        final ExtDebugTrace extDebugTrace = deepDebugLog.isDeepDebugEnabled() ? toExtDebugTrace(deepDebugLog) : null;

        return httpCalls == null && bidRequest == null && extDebugPgmetrics == null && extDebugTrace == null
                ? null
                : ExtResponseDebug.of(httpCalls, bidRequest, extDebugPgmetrics, extDebugTrace);
    }

    /**
     * Returns new {@link HashSet} in case of existing keywordsCreator or empty collection if null.
     */
    private static Set<Bid> newOrEmptySet(ExtRequestTargeting targeting) {
        return targeting != null ? new HashSet<>() : Collections.emptySet();
    }

    private static Stream<Bid> getBids(BidderResponse bidderResponse) {
        return Stream.of(bidderResponse)
                .map(BidderResponse::getSeatBid)
                .filter(Objects::nonNull)
                .map(BidderSeatBid::getBids)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .map(BidderBid::getBid);
    }

    /**
     * Corresponds cacheId (or null if not present) to each {@link Bid}.
     */
    private Future<CacheServiceResult> toBidsWithCacheIds(List<BidderResponse> bidderResponses,
                                                          Set<Bid> bidsToCache,
                                                          AuctionContext auctionContext,
                                                          BidRequestCacheInfo cacheInfo,
                                                          EventsContext eventsContext) {

        if (!cacheInfo.isDoCaching()) {
            return Future.succeededFuture(CacheServiceResult.of(null, null, toMapBidsWithEmptyCacheIds(bidsToCache)));
        }

        // do not submit bids with zero price to prebid cache
        final List<Bid> bidsWithNonZeroPrice = bidsToCache.stream()
                .filter(bid -> bid.getPrice().compareTo(BigDecimal.ZERO) > 0)
                .collect(Collectors.toList());

        final boolean shouldCacheVideoBids = cacheInfo.isShouldCacheVideoBids();

        final Map<String, List<String>> bidderToVideoBidIdsToModify =
                shouldCacheVideoBids && eventsEnabledForAccount(auctionContext)
                        ? getBidderAndVideoBidIdsToModify(bidderResponses, auctionContext.getBidRequest().getImp())
                        : Collections.emptyMap();
        final Map<String, List<String>> bidderToBidIds = bidderResponses.stream()
                .collect(Collectors.toMap(BidderResponse::getBidder, bidderResponse -> getBids(bidderResponse)
                        .map(Bid::getId)
                        .collect(Collectors.toList())));

        final CacheContext cacheContext = CacheContext.builder()
                .cacheBidsTtl(cacheInfo.getCacheBidsTtl())
                .cacheVideoBidsTtl(cacheInfo.getCacheVideoBidsTtl())
                .shouldCacheBids(cacheInfo.isShouldCacheBids())
                .shouldCacheVideoBids(shouldCacheVideoBids)
                .bidderToVideoBidIdsToModify(bidderToVideoBidIdsToModify)
                .bidderToBidIds(bidderToBidIds)
                .build();

        return cacheService.cacheBidsOpenrtb(bidsWithNonZeroPrice, auctionContext, cacheContext, eventsContext)
                .map(cacheResult -> addNotCachedBids(cacheResult, bidsToCache));
    }

    private Map<String, List<String>> getBidderAndVideoBidIdsToModify(List<BidderResponse> bidderResponses,
                                                                      List<Imp> imps) {

        return bidderResponses.stream()
                .filter(bidderResponse -> bidderCatalog.isModifyingVastXmlAllowed(bidderResponse.getBidder()))
                .collect(Collectors.toMap(BidderResponse::getBidder, bidderResponse -> getBids(bidderResponse)
                        .filter(bid -> isVideoBid(bid, imps))
                        .map(Bid::getId)
                        .collect(Collectors.toList())));
    }

    private static boolean isVideoBid(Bid bid, List<Imp> imps) {
        return imps.stream()
                .filter(imp -> imp.getVideo() != null)
                .map(Imp::getId)
                .anyMatch(impId -> bid.getImpid().equals(impId));
    }

    /**
     * Creates a map with {@link Bid} as a key and null as a value.
     */
    private static Map<Bid, CacheIdInfo> toMapBidsWithEmptyCacheIds(Set<Bid> bids) {
        return bids.stream()
                .collect(Collectors.toMap(Function.identity(), ignored -> CacheIdInfo.empty()));
    }

    /**
     * Adds bids with no cache id info.
     */
    private static CacheServiceResult addNotCachedBids(CacheServiceResult cacheResult, Set<Bid> bids) {
        final Map<Bid, CacheIdInfo> bidToCacheIdInfo = cacheResult.getCacheBids();

        if (bids.size() > bidToCacheIdInfo.size()) {
            final Map<Bid, CacheIdInfo> updatedBidToCacheIdInfo = new HashMap<>(bidToCacheIdInfo);
            for (Bid bid : bids) {
                if (!updatedBidToCacheIdInfo.containsKey(bid)) {
                    updatedBidToCacheIdInfo.put(bid, CacheIdInfo.empty());
                }
            }
            return CacheServiceResult.of(cacheResult.getHttpCall(), cacheResult.getError(), updatedBidToCacheIdInfo);
        }
        return cacheResult;
    }

    private static Map<String, List<ExtHttpCall>> toExtHttpCalls(List<BidderResponse> bidderResponses,
                                                                 CacheServiceResult cacheResult,
                                                                 Map<String, List<DebugHttpCall>> contextHttpCalls) {

        final Map<String, List<ExtHttpCall>> bidderHttpCalls = bidderResponses.stream()
                .collect(Collectors.toMap(BidderResponse::getBidder,
                        bidderResponse -> ListUtils.emptyIfNull(bidderResponse.getSeatBid().getHttpCalls())));

        final DebugHttpCall httpCall = cacheResult.getHttpCall();
        final ExtHttpCall cacheExtHttpCall = httpCall != null ? toExtHttpCall(httpCall) : null;
        final Map<String, List<ExtHttpCall>> cacheHttpCalls = cacheExtHttpCall != null
                ? Collections.singletonMap(CACHE, Collections.singletonList(cacheExtHttpCall))
                : Collections.emptyMap();

        final Map<String, List<ExtHttpCall>> contextExtHttpCalls = contextHttpCalls.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, serviceToHttpCall -> serviceToHttpCall.getValue().stream()
                        .map(BidResponseCreator::toExtHttpCall)
                        .collect(Collectors.toList())));

        final Map<String, List<ExtHttpCall>> httpCalls = new HashMap<>();
        httpCalls.putAll(bidderHttpCalls);
        httpCalls.putAll(cacheHttpCalls);
        httpCalls.putAll(contextExtHttpCalls);
        return httpCalls.isEmpty() ? null : httpCalls;
    }

    private static ExtHttpCall toExtHttpCall(DebugHttpCall debugHttpCall) {
        return ExtHttpCall.builder()
                .uri(debugHttpCall.getRequestUri())
                .requestbody(debugHttpCall.getRequestBody())
                .status(debugHttpCall.getResponseStatus())
                .responsebody(debugHttpCall.getResponseBody())
                .build();
    }

    private static ExtDebugPgmetrics toExtDebugPgmetrics(TxnLog txnLog) {
        final ExtDebugPgmetrics extDebugPgmetrics = ExtDebugPgmetrics.builder()
                .matchedDomainTargeting(nullIfEmpty(txnLog.lineItemsMatchedDomainTargeting()))
                .matchedWholeTargeting(nullIfEmpty(txnLog.lineItemsMatchedWholeTargeting()))
                .matchedTargetingFcapped(nullIfEmpty(txnLog.lineItemsMatchedTargetingFcapped()))
                .matchedTargetingFcapLookupFailed(nullIfEmpty(txnLog.lineItemsMatchedTargetingFcapLookupFailed()))
                .readyToServe(nullIfEmpty(txnLog.lineItemsReadyToServe()))
                .pacingDeferred(nullIfEmpty(txnLog.lineItemsPacingDeferred()))
                .sentToBidder(nullIfEmpty(txnLog.lineItemsSentToBidder()))
                .sentToBidderAsTopMatch(nullIfEmpty(txnLog.lineItemsSentToBidderAsTopMatch()))
                .receivedFromBidder(nullIfEmpty(txnLog.lineItemsReceivedFromBidder()))
                .responseInvalidated(nullIfEmpty(txnLog.lineItemsResponseInvalidated()))
                .sentToClient(nullIfEmpty(txnLog.lineItemsSentToClient()))
                .sentToClientAsTopMatch(nullIfEmpty(txnLog.lineItemSentToClientAsTopMatch()))
                .build();
        return extDebugPgmetrics.equals(ExtDebugPgmetrics.EMPTY) ? null : extDebugPgmetrics;
    }

    private static ExtDebugTrace toExtDebugTrace(DeepDebugLog deepDebugLog) {
        final List<ExtTraceDeal> entries = deepDebugLog.entries();
        final List<ExtTraceDeal> dealsTrace = entries.stream()
                .filter(extTraceDeal -> StringUtils.isEmpty(extTraceDeal.getLineItemId()))
                .collect(Collectors.toList());
        final Map<String, List<ExtTraceDeal>> lineItemsTrace = entries.stream()
                .filter(extTraceDeal -> StringUtils.isNotEmpty(extTraceDeal.getLineItemId()))
                .collect(Collectors.groupingBy(ExtTraceDeal::getLineItemId, Collectors.toList()));
        return CollectionUtils.isNotEmpty(entries)
                ? ExtDebugTrace.of(CollectionUtils.isEmpty(dealsTrace) ? null : dealsTrace,
                MapUtils.isEmpty(lineItemsTrace) ? null : lineItemsTrace)
                : null;
    }

    private Map<String, List<ExtBidderError>> toExtBidderErrors(List<BidderResponse> bidderResponses,
                                                                AuctionContext auctionContext,
                                                                CacheServiceResult cacheResult,
                                                                VideoStoredDataResult videoStoredDataResult,
                                                                Map<String, List<ExtBidderError>> bidErrors) {
        final BidRequest bidRequest = auctionContext.getBidRequest();
        final Map<String, List<ExtBidderError>> errors = new HashMap<>();

        errors.putAll(extractBidderErrors(bidderResponses));
        errors.putAll(extractDeprecatedBiddersErrors(bidRequest));
        errors.putAll(extractPrebidErrors(cacheResult, videoStoredDataResult, auctionContext));
        if (MapUtils.isNotEmpty(bidErrors)) {
            addBidErrors(errors, bidErrors);
        }

        return errors.isEmpty() ? null : errors;
    }

    /**
     * Returns a map with bidder name as a key and list of {@link ExtBidderError}s as a value.
     */
    private static Map<String, List<ExtBidderError>> extractBidderErrors(List<BidderResponse> bidderResponses) {
        return bidderResponses.stream()
                .filter(bidderResponse -> CollectionUtils.isNotEmpty(bidderResponse.getSeatBid().getErrors()))
                .collect(Collectors.toMap(BidderResponse::getBidder,
                        bidderResponse -> errorsDetails(bidderResponse.getSeatBid().getErrors())));
    }

    /**
     * Maps a list of {@link BidderError} to a list of {@link ExtBidderError}s.
     */
    private static List<ExtBidderError> errorsDetails(List<BidderError> errors) {
        return errors.stream()
                .map(bidderError -> ExtBidderError.of(bidderError.getType().getCode(), bidderError.getMessage(),
                        CollectionUtils.isNotEmpty(bidderError.getImpIds()) ? bidderError.getImpIds() : null))
                .collect(Collectors.toList());
    }

    /**
     * Returns a map with deprecated bidder name as a key and list of {@link ExtBidderError}s as a value.
     */
    private Map<String, List<ExtBidderError>> extractDeprecatedBiddersErrors(BidRequest bidRequest) {
        return bidRequest.getImp().stream()
                .filter(imp -> imp.getExt() != null)
                .flatMap(imp -> asStream(imp.getExt().fieldNames()))
                .distinct()
                .filter(bidderCatalog::isDeprecatedName)
                .collect(Collectors.toMap(Function.identity(),
                        bidder -> Collections.singletonList(ExtBidderError.of(BidderError.Type.bad_input.getCode(),
                                bidderCatalog.errorForDeprecatedName(bidder)))));
    }

    /**
     * Returns a singleton map with "prebid" as a key and list of {@link ExtBidderError}s cache errors as a value.
     */
    private static Map<String, List<ExtBidderError>> extractPrebidErrors(CacheServiceResult cacheResult,
                                                                         VideoStoredDataResult videoStoredDataResult,
                                                                         AuctionContext auctionContext) {
        final List<ExtBidderError> cacheErrors = extractCacheErrors(cacheResult);
        final List<ExtBidderError> storedErrors = extractStoredErrors(videoStoredDataResult);
        final List<ExtBidderError> contextErrors = extractContextErrors(auctionContext);
        if (cacheErrors.isEmpty() && storedErrors.isEmpty() && contextErrors.isEmpty()) {
            return Collections.emptyMap();
        }

        final List<ExtBidderError> collectedErrors = Stream.concat(contextErrors.stream(),
                Stream.concat(storedErrors.stream(), cacheErrors.stream()))
                .collect(Collectors.toList());
        return Collections.singletonMap(PREBID_EXT, collectedErrors);
    }

    /**
     * Returns a list of {@link ExtBidderError}s of cache errors.
     */
    private static List<ExtBidderError> extractCacheErrors(CacheServiceResult cacheResult) {
        final Throwable error = cacheResult.getError();
        if (error != null) {
            final ExtBidderError extBidderError = ExtBidderError.of(BidderError.Type.generic.getCode(),
                    error.getMessage());
            return Collections.singletonList(extBidderError);
        }
        return Collections.emptyList();
    }

    /**
     * Returns a list of {@link ExtBidderError}s of stored request errors.
     */
    private static List<ExtBidderError> extractStoredErrors(VideoStoredDataResult videoStoredDataResult) {
        final List<String> errors = videoStoredDataResult.getErrors();
        if (CollectionUtils.isNotEmpty(errors)) {
            return errors.stream()
                    .map(message -> ExtBidderError.of(BidderError.Type.generic.getCode(), message))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /**
     * Returns a list of {@link ExtBidderError}s of auction context prebid errors.
     */
    private static List<ExtBidderError> extractContextErrors(AuctionContext auctionContext) {
        return auctionContext.getPrebidErrors().stream()
                .map(message -> ExtBidderError.of(BidderError.Type.generic.getCode(), message))
                .collect(Collectors.toList());
    }

    /**
     * Adds bid errors: if value by key exists - add errors to its list, otherwise - add an entry.
     */
    private static void addBidErrors(Map<String, List<ExtBidderError>> errors,
                                     Map<String, List<ExtBidderError>> bidErrors) {
        for (Map.Entry<String, List<ExtBidderError>> errorEntry : bidErrors.entrySet()) {
            final List<ExtBidderError> extBidderErrors = errors.get(errorEntry.getKey());
            if (extBidderErrors != null) {
                extBidderErrors.addAll(errorEntry.getValue());
            } else {
                errors.put(errorEntry.getKey(), errorEntry.getValue());
            }
        }
    }

    private static <T> Stream<T> asStream(Iterator<T> iterator) {
        final Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Returns a map with response time by bidders and cache.
     */
    private static Map<String, Integer> toResponseTimes(List<BidderResponse> bidderResponses,
                                                        CacheServiceResult cacheResult) {
        final Map<String, Integer> responseTimeMillis = bidderResponses.stream()
                .collect(Collectors.toMap(BidderResponse::getBidder, BidderResponse::getResponseTime));

        final DebugHttpCall debugHttpCall = cacheResult.getHttpCall();
        final Integer cacheResponseTime = debugHttpCall != null ? debugHttpCall.getResponseTimeMillis() : null;
        if (cacheResponseTime != null) {
            responseTimeMillis.put(CACHE, cacheResponseTime);
        }
        return responseTimeMillis;
    }

    /**
     * Returns {@link BidResponse} based on list of {@link BidderResponse}s and {@link CacheServiceResult}.
     */
    private BidResponse toBidResponse(List<BidderResponse> bidderResponses,
                                      AuctionContext auctionContext,
                                      ExtRequestTargeting targeting,
                                      Set<Bid> winningBids,
                                      Set<Bid> winningBidsByBidder,
                                      BidRequestCacheInfo cacheInfo,
                                      CacheServiceResult cacheResult,
                                      VideoStoredDataResult videoStoredDataResult,
                                      EventsContext eventsContext,
                                      long auctionTimestamp,
                                      boolean debugEnabled,
                                      List<Imp> imps) {

        final BidRequest bidRequest = auctionContext.getBidRequest();
        final Account account = auctionContext.getAccount();

        final Map<String, List<ExtBidderError>> bidErrors = new HashMap<>();
        final List<SeatBid> seatBids = bidderResponses.stream()
                .filter(bidderResponse -> !bidderResponse.getSeatBid().getBids().isEmpty())
                .map(bidderResponse -> toSeatBid(
                        bidderResponse,
                        targeting,
                        bidRequest,
                        winningBids,
                        winningBidsByBidder,
                        cacheInfo,
                        cacheResult.getCacheBids(),
                        videoStoredDataResult,
                        account,
                        bidErrors,
                        eventsContext,
                        imps))
                .collect(Collectors.toList());

        final ExtBidResponse extBidResponse = toExtBidResponse(
                bidderResponses,
                auctionContext,
                cacheResult,
                videoStoredDataResult,
                auctionTimestamp,
                debugEnabled,
                bidErrors);

        return BidResponse.builder()
                .id(bidRequest.getId())
                .cur(bidRequest.getCur().get(0))
                .seatbid(seatBids)
                .ext(mapper.mapper().valueToTree(extBidResponse))
                .build();
    }

    /**
     * Populates 2 input sets:
     * <p>
     * - winning bids for each impId (ad unit code) through all bidder responses.
     * <br>
     * - winning bids for each impId but for separate bidder.
     * <p>
     * Winning bid is the one with the highest price.
     */
    private void populateWinningBids(List<BidderResponse> bidderResponses,
                                     Set<Bid> winningBids,
                                     Set<Bid> winningBidsByBidder,
                                     List<Imp> imps,
                                     TxnLog txnLog) {

        final Map<String, Bid> winningBidsMap = new HashMap<>(); // impId -> Bid
        final Map<String, Map<String, Bid>> winningBidsByBidderMap = new HashMap<>(); // impId -> [bidder -> Bid]
        final Map<String, Set<String>> impToLineItemIds = MapUtils.lazyMap(new HashMap<>(),
                (Factory<Set<String>>) HashSet::new);

        for (BidderResponse bidderResponse : bidderResponses) {
            final String bidder = bidderResponse.getBidder();

            for (BidderBid bidderBid : bidderResponse.getSeatBid().getBids()) {
                final Bid bid = bidderBid.getBid();
                populateImpToLineItemIds(imps, impToLineItemIds, bid);
                tryAddWinningBid(bid, winningBidsMap, imps);
                tryAddWinningBidByBidder(bid, bidder, winningBidsByBidderMap, imps);
            }
        }

        winningBids.addAll(winningBidsMap.values());

        updateAuctionLostToMetric(winningBids, imps, txnLog, impToLineItemIds);

        final List<Bid> bidsByBidder = winningBidsByBidderMap.values().stream()
                .flatMap(bidsByBidderMap -> bidsByBidderMap.values().stream())
                .collect(Collectors.toList());
        winningBidsByBidder.addAll(bidsByBidder);
    }

    /**
     * Populates map with imp ids to corresponding line item ids.
     */
    private void populateImpToLineItemIds(List<Imp> imps, Map<String, Set<String>> impToLineItemIds, Bid bid) {
        final String lineItemId = LineItemUtil.lineItemIdFrom(bid, imps, mapper);
        final String impId = bid.getImpid();
        if (lineItemId != null) {
            impToLineItemIds.get(impId).add(lineItemId);
        }
    }

    /**
     * Updates auction lost to line item metric.
     */
    private void updateAuctionLostToMetric(Set<Bid> winningBids, List<Imp> imps, TxnLog txnLog,
                                           Map<String, Set<String>> impToLineItemIds) {
        for (final Bid winningBid : winningBids) {
            final String winningBidImpId = winningBid.getImpid();
            final String winningLineItemId = LineItemUtil.lineItemIdFrom(winningBid, imps, mapper);
            if (winningLineItemId != null) {
                impToLineItemIds.get(winningBidImpId).stream()
                        .filter(lineItemId -> !Objects.equals(lineItemId, winningLineItemId))
                        .forEach(lineItemId ->
                                txnLog.lostAuctionToLineItems().get(lineItemId).add(winningLineItemId));
            }
        }
    }

    /**
     * Tries to add a winning bid for each impId.
     */
    private static void tryAddWinningBid(Bid bid, Map<String, Bid> winningBids, List<Imp> imps) {
        final String impId = bid.getImpid();

        if (!winningBids.containsKey(impId) || isWinningBid(bid, winningBids.get(impId), imps)) {
            winningBids.put(impId, bid);
        }
    }

    /**
     * Tries to add a winning bid for each impId for separate bidder.
     */
    private static void tryAddWinningBidByBidder(Bid bid, String bidder,
                                                 Map<String, Map<String, Bid>> winningBidsByBidder, List<Imp> imps) {
        final String impId = bid.getImpid();

        if (!winningBidsByBidder.containsKey(impId)) {
            final Map<String, Bid> bidsByBidder = new HashMap<>();
            bidsByBidder.put(bidder, bid);

            winningBidsByBidder.put(impId, bidsByBidder);
        } else {
            final Map<String, Bid> bidsByBidder = winningBidsByBidder.get(impId);

            if (!bidsByBidder.containsKey(bidder) || isWinningBid(bid, bidsByBidder.get(bidder), imps)) {
                bidsByBidder.put(bidder, bid);
            }
        }
    }

    /**
     * Returns true if the first given {@link Bid} wins the previous winner, otherwise false.
     * <p>
     * The priority for choosing the 'winner' (hb_pb, hb_bidder, etc) is:
     * <p>
     * - PG Line Items always win over non-PG bids
     * - Amongst PG Line Items, choose the highest CPM
     * - Amongst non-PG bids, choose the highest CPM
     */
    private static boolean isWinningBid(Bid bid, Bid otherBid, List<Imp> imps) {
        final boolean result;

        final String bidDealId = bid.getDealid();
        final String otherBidDealId = otherBid.getDealid();

        if (bidDealId != null) {
            if (otherBidDealId == null) {
                result = true;
            } else { // another Bid has deal ID too
                result = isWinnerBidByDeal(bid, otherBid, imps);
            }
        } else if (otherBidDealId != null) {
            result = false;
        } else {
            result = isWinnerBidByPrice(bid, otherBid);
        }

        return result;
    }

    /**
     * Returns true if the first given {@link Bid} has higher deal priority than another one, otherwise false.
     * <p>
     * Compares by CPM if deal comparing failed.
     */
    private static boolean isWinnerBidByDeal(Bid bid, Bid otherBid, List<Imp> imps) {
        final boolean result;

        final Imp imp = imps.stream()
                .filter(foundImp -> Objects.equals(foundImp.getId(), bid.getImpid()))
                .findFirst()
                // this should never happen - response validator covers the case
                .orElseThrow(() -> new IllegalStateException(
                        String.format("Error while determining winning bid: "
                                + "No corresponding imp was found for bid with id: %s", bid.getId())));

        final Pmp pmp = imp.getPmp();
        final List<Deal> deals = pmp != null ? pmp.getDeals() : null;
        if (CollectionUtils.isEmpty(deals)) {
            result = isWinnerBidByPrice(bid, otherBid);
        } else {
            int indexOfBidDealId = -1;
            int indexOfOtherBidDealId = -1;

            // search for indexes of deals
            for (int i = 0; i < deals.size(); i++) {
                final String dealId = deals.get(i).getId();
                if (Objects.equals(dealId, bid.getDealid())) {
                    indexOfBidDealId = i;
                }
                if (Objects.equals(dealId, otherBid.getDealid())) {
                    indexOfOtherBidDealId = i;
                }
            }

            if (indexOfBidDealId == -1 || indexOfOtherBidDealId == -1) { // one or both deal IDs not found
                result = indexOfBidDealId != -1; // case when no deal IDs found is covered by response validator
            } else if (indexOfBidDealId == indexOfOtherBidDealId) { // deal IDs is not unique
                result = isWinnerBidByPrice(bid, otherBid);
            } else {
                result = indexOfBidDealId < indexOfOtherBidDealId;
            }
        }

        return result;
    }

    /**
     * Returns true if the first given {@link Bid} has higher CPM than another one, otherwise false.
     */
    private static boolean isWinnerBidByPrice(Bid bid, Bid otherBid) {
        return bid.getPrice().compareTo(otherBid.getPrice()) > 0;
    }

    private Future<VideoStoredDataResult> videoStoredDataResult(List<Imp> imps, Timeout timeout) {
        final List<String> errors = new ArrayList<>();
        final List<Imp> storedVideoInjectableImps = new ArrayList<>();
        for (Imp imp : imps) {
            try {
                if (checkEchoVideoAttrs(imp)) {
                    storedVideoInjectableImps.add(imp);
                }
            } catch (InvalidRequestException e) {
                errors.add(e.getMessage());
            }
        }

        return storedRequestProcessor.videoStoredDataResult(storedVideoInjectableImps, errors, timeout)
                .otherwise(throwable -> VideoStoredDataResult.of(Collections.emptyMap(),
                        Collections.singletonList(throwable.getMessage())));
    }

    /**
     * Checks if imp.ext.prebid.options.echovideoattrs equals true.
     */
    private boolean checkEchoVideoAttrs(Imp imp) {
        if (imp.getExt() != null) {
            try {
                final ExtImp extImp = mapper.mapper().treeToValue(imp.getExt(), ExtImp.class);
                final ExtImpPrebid prebid = extImp.getPrebid();
                final ExtOptions options = prebid != null ? prebid.getOptions() : null;
                final Boolean echoVideoAttrs = options != null ? options.getEchoVideoAttrs() : null;
                return BooleanUtils.toBoolean(echoVideoAttrs);
            } catch (JsonProcessingException e) {
                throw new InvalidRequestException(String.format(
                        "Incorrect Imp extension format for Imp with id %s: %s", imp.getId(), e.getMessage()));
            }
        }
        return false;
    }

    /**
     * Creates an OpenRTB {@link SeatBid} for a bidder. It will contain all the bids supplied by a bidder and a "bidder"
     * extension field populated.
     */
    private SeatBid toSeatBid(BidderResponse bidderResponse,
                              ExtRequestTargeting targeting,
                              BidRequest bidRequest,
                              Set<Bid> winningBids,
                              Set<Bid> winningBidsByBidder,
                              BidRequestCacheInfo cacheInfo,
                              Map<Bid, CacheIdInfo> cachedBids,
                              VideoStoredDataResult videoStoredDataResult,
                              Account account,
                              Map<String, List<ExtBidderError>> bidErrors,
                              EventsContext eventsContext,
                              List<Imp> imps) {

        final String bidder = bidderResponse.getBidder();

        final List<Bid> bids = bidderResponse.getSeatBid().getBids().stream()
                .map(bidderBid -> toBid(
                        bidderBid,
                        bidder,
                        targeting,
                        bidRequest,
                        winningBids,
                        winningBidsByBidder,
                        cacheInfo,
                        cachedBids,
                        videoStoredDataResult.getImpIdToStoredVideo(),
                        account,
                        eventsContext,
                        bidErrors,
                        imps))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return SeatBid.builder()
                .seat(bidder)
                .bid(bids)
                .group(0) // prebid cannot support roadblocking
                .build();
    }

    /**
     * Returns an OpenRTB {@link Bid} with "prebid" and "bidder" extension fields populated.
     */
    private Bid toBid(BidderBid bidderBid,
                      String bidder,
                      ExtRequestTargeting targeting,
                      BidRequest bidRequest,
                      Set<Bid> winningBids,
                      Set<Bid> winningBidsByBidder,
                      BidRequestCacheInfo cacheInfo,
                      Map<Bid, CacheIdInfo> bidsWithCacheIds,
                      Map<String, Video> impIdToStoredVideo,
                      Account account,
                      EventsContext eventsContext,
                      Map<String, List<ExtBidderError>> bidErrors,
                      List<Imp> imps) {

        final Bid bid = bidderBid.getBid();
        final BidType bidType = bidderBid.getType();

        final boolean isApp = bidRequest.getApp() != null;
        if (isApp && bidType.equals(BidType.xNative)) {
            try {
                addNativeMarkup(bid, bidRequest.getImp());
            } catch (PreBidException e) {
                bidErrors.putIfAbsent(bidder, new ArrayList<>());
                bidErrors.get(bidder)
                        .add(ExtBidderError.of(BidderError.Type.bad_server_response.getCode(), e.getMessage()));
                return null;
            }
        }

        final CacheIdInfo cacheIdInfo = bidsWithCacheIds.get(bid);
        final String cacheId = cacheIdInfo != null ? cacheIdInfo.getCacheId() : null;
        final String videoCacheId = cacheIdInfo != null ? cacheIdInfo.getVideoCacheId() : null;

        if ((videoCacheId != null && !cacheInfo.isReturnCreativeVideoBids())
                || (cacheId != null && !cacheInfo.isReturnCreativeBids())) {
            bid.setAdm(null);
        }

        final Map<String, String> targetingKeywords;
        final ExtResponseCache cache;

        if (targeting != null && winningBidsByBidder.contains(bid)) {
            final TargetingKeywordsCreator keywordsCreator = keywordsCreator(targeting, isApp, bidRequest, account);
            final Map<BidType, TargetingKeywordsCreator> keywordsCreatorByBidType =
                    keywordsCreatorByBidType(targeting, isApp, bidRequest, account);
            final boolean isWinningBid = winningBids.contains(bid);
            final String lineItemSource = getLineItemSource(imps, bid);
            targetingKeywords = keywordsCreatorByBidType.getOrDefault(bidType, keywordsCreator)
                    .makeFor(bid, bidder, isWinningBid, cacheId, videoCacheId, lineItemSource);

            final CacheAsset bids = cacheId != null ? toCacheAsset(cacheId) : null;
            final CacheAsset vastXml = videoCacheId != null ? toCacheAsset(videoCacheId) : null;
            cache = bids != null || vastXml != null ? ExtResponseCache.of(bids, vastXml) : null;
        } else {
            targetingKeywords = null;
            cache = null;
        }

        final String generatedBidId = generateBidId ? UUID.randomUUID().toString() : null;
        final String eventBidId = ObjectUtils.defaultIfNull(generatedBidId, bid.getId());
        final Video storedVideo = impIdToStoredVideo.get(bid.getImpid());
        final Events events = createEvents(bidder, account, eventBidId, eventsContext, bid, imps);
        final ExtBidPrebidVideo extBidPrebidVideo = getExtBidPrebidVideo(bid.getExt());
        final ExtBidPrebid prebidExt = ExtBidPrebid.builder()
                .bidid(generatedBidId)
                .type(bidType)
                .targeting(targetingKeywords)
                .cache(cache)
                .storedRequestAttributes(storedVideo)
                .events(events)
                .video(extBidPrebidVideo)
                .build();

        final ExtPrebid<ExtBidPrebid, ObjectNode> bidExt = ExtPrebid.of(prebidExt, bid.getExt());
        bid.setExt(mapper.mapper().valueToTree(bidExt));

        return bid;
    }

    private String getLineItemSource(List<Imp> imps, Bid bid) {
        return bid.getDealid() != null
                ? LineItemUtil.lineItemSourceFrom(bid, imps, mapper)
                : null;
    }

    private void addNativeMarkup(Bid bid, List<Imp> imps) {
        final Response nativeMarkup;
        try {
            nativeMarkup = mapper.decodeValue(bid.getAdm(), Response.class);
        } catch (DecodeException e) {
            throw new PreBidException(e.getMessage());
        }

        final List<Asset> responseAssets = nativeMarkup.getAssets();
        if (CollectionUtils.isNotEmpty(responseAssets)) {
            final Native nativeImp = imps.stream()
                    .filter(imp -> imp.getId().equals(bid.getImpid()) && imp.getXNative() != null)
                    .findFirst()
                    .map(Imp::getXNative)
                    .orElseThrow(() -> new PreBidException("Could not find native imp"));

            final Request nativeRequest;
            try {
                nativeRequest = mapper.mapper().readValue(nativeImp.getRequest(), Request.class);
            } catch (JsonProcessingException e) {
                throw new PreBidException(e.getMessage());
            }

            responseAssets.forEach(asset -> setAssetTypes(asset, nativeRequest.getAssets()));
            bid.setAdm(mapper.encode(nativeMarkup));
        }
    }

    private static void setAssetTypes(Asset responseAsset, List<com.iab.openrtb.request.Asset> requestAssets) {
        if (responseAsset.getImg() != null) {
            final ImageObject img = getAssetById(responseAsset.getId(), requestAssets).getImg();
            final Integer type = img != null ? img.getType() : null;
            if (type != null) {
                responseAsset.getImg().setType(type);
            } else {
                throw new PreBidException(String.format("Response has an Image asset with ID:%s present that doesn't "
                        + "exist in the request", responseAsset.getId()));
            }
        }
        if (responseAsset.getData() != null) {
            final DataObject data = getAssetById(responseAsset.getId(), requestAssets).getData();
            final Integer type = data != null ? data.getType() : null;
            if (type != null) {
                responseAsset.getData().setType(type);
            } else {
                throw new PreBidException(String.format("Response has a Data asset with ID:%s present that doesn't "
                        + "exist in the request", responseAsset.getId()));
            }
        }
    }

    private static com.iab.openrtb.request.Asset getAssetById(int assetId,
                                                              List<com.iab.openrtb.request.Asset> requestAssets) {
        return requestAssets.stream()
                .filter(asset -> asset.getId() == assetId)
                .findFirst()
                .orElse(com.iab.openrtb.request.Asset.EMPTY);
    }

    private Events createEvents(String bidder,
                                Account account,
                                String eventBidId,
                                EventsContext eventsContext,
                                Bid bid,
                                List<Imp> imps) {

        if (!eventsContext.isEnabledForAccount()) {
            return null;
        }

        final String lineItemId = LineItemUtil.lineItemIdFrom(bid, imps, mapper);

        return eventsContext.isEnabledForRequest() || StringUtils.isNotEmpty(lineItemId)
                ? eventsService.createEvent(
                eventBidId,
                bidder,
                account.getId(),
                lineItemId,
                eventsContext.isEnabledForRequest(),
                eventsContext.getAuctionTimestamp(),
                eventsContext.getIntegration())
                : null;
    }

    private static boolean eventsEnabledForAccount(AuctionContext auctionContext) {
        return BooleanUtils.isTrue(auctionContext.getAccount().getEventsEnabled());
    }

    private static boolean eventsEnabledForRequest(AuctionContext auctionContext) {
        return eventsEnabledForChannel(auctionContext) || eventsAllowedByRequest(auctionContext);
    }

    private static boolean eventsEnabledForChannel(AuctionContext auctionContext) {
        final AccountAnalyticsConfig analyticsConfig = ObjectUtils.defaultIfNull(
                auctionContext.getAccount().getAnalyticsConfig(), AccountAnalyticsConfig.fallback());
        final Map<String, Boolean> channelConfig = analyticsConfig.getAuctionEvents();

        final String channelFromRequest = channelFromRequest(auctionContext.getBidRequest());

        return MapUtils.emptyIfNull(channelConfig).entrySet().stream()
                .filter(entry -> StringUtils.equalsIgnoreCase(channelFromRequest, entry.getKey()))
                .findFirst()
                .map(entry -> BooleanUtils.isTrue(entry.getValue()))
                .orElse(Boolean.FALSE);
    }

    private static String channelFromRequest(BidRequest bidRequest) {
        final ExtRequest requestExt = bidRequest.getExt();
        final ExtRequestPrebid prebid = requestExt != null ? requestExt.getPrebid() : null;
        final ExtRequestPrebidChannel channel = prebid != null ? prebid.getChannel() : null;

        return channel != null ? channel.getName() : null;
    }

    private static boolean eventsAllowedByRequest(AuctionContext auctionContext) {
        final ExtRequest requestExt = auctionContext.getBidRequest().getExt();
        final ExtRequestPrebid prebid = requestExt != null ? requestExt.getPrebid() : null;

        return prebid != null && prebid.getEvents() != null;
    }

    /**
     * Extracts targeting keywords settings from the bid request and creates {@link TargetingKeywordsCreator}
     * instance if it is present.
     */
    private TargetingKeywordsCreator keywordsCreator(
            ExtRequestTargeting targeting, boolean isApp, BidRequest bidRequest, Account account) {

        final JsonNode priceGranularityNode = targeting.getPricegranularity();
        return priceGranularityNode == null || priceGranularityNode.isNull()
                ? null
                : createKeywordsCreator(targeting, isApp, priceGranularityNode, bidRequest, account);
    }

    /**
     * Returns a map of {@link BidType} to correspondent {@link TargetingKeywordsCreator}
     * extracted from {@link ExtRequestTargeting} if it exists.
     */
    private Map<BidType, TargetingKeywordsCreator> keywordsCreatorByBidType(ExtRequestTargeting targeting,
                                                                            boolean isApp,
                                                                            BidRequest bidRequest,
                                                                            Account account) {

        final ExtMediaTypePriceGranularity mediaTypePriceGranularity = targeting.getMediatypepricegranularity();

        if (mediaTypePriceGranularity == null) {
            return Collections.emptyMap();
        }

        final Map<BidType, TargetingKeywordsCreator> result = new EnumMap<>(BidType.class);
        final int resolvedTruncateAttrChars = resolveTruncateAttrChars(targeting, account);

        final ObjectNode banner = mediaTypePriceGranularity.getBanner();
        final boolean isBannerNull = banner == null || banner.isNull();
        if (!isBannerNull) {
            result.put(BidType.banner, createKeywordsCreator(targeting, isApp, banner, bidRequest, account));
        }

        final ObjectNode video = mediaTypePriceGranularity.getVideo();
        final boolean isVideoNull = video == null || video.isNull();
        if (!isVideoNull) {
            result.put(BidType.video, createKeywordsCreator(targeting, isApp, video, bidRequest, account));
        }

        final ObjectNode xNative = mediaTypePriceGranularity.getXNative();
        final boolean isNativeNull = xNative == null || xNative.isNull();
        if (!isNativeNull) {
            result.put(BidType.xNative, createKeywordsCreator(targeting, isApp, xNative, bidRequest, account));
        }

        return result;
    }

    private TargetingKeywordsCreator createKeywordsCreator(ExtRequestTargeting targeting,
                                                           boolean isApp,
                                                           JsonNode priceGranularity,
                                                           BidRequest bidRequest,
                                                           Account account) {

        return TargetingKeywordsCreator.create(
                parsePriceGranularity(priceGranularity),
                targeting.getIncludewinners(),
                targeting.getIncludebidderkeys(),
                isApp,
                resolveTruncateAttrChars(targeting, account),
                cacheHost,
                cachePath,
                TargetingKeywordsResolver.create(bidRequest, mapper));
    }

    /**
     * Returns max targeting keyword length.
     */
    private int resolveTruncateAttrChars(ExtRequestTargeting targeting, Account account) {
        return ObjectUtils.firstNonNull(
                truncateAttrCharsOrNull(targeting.getTruncateattrchars()),
                truncateAttrCharsOrNull(account.getTruncateTargetAttr()),
                truncateAttrChars);
    }

    private static Integer truncateAttrCharsOrNull(Integer value) {
        return value != null && value >= 0 && value <= 255 ? value : null;
    }

    /**
     * Parse {@link JsonNode} to {@link List} of {@link ExtPriceGranularity}.
     * <p>
     * Throws {@link PreBidException} in case of errors during decoding price granularity.
     */
    private ExtPriceGranularity parsePriceGranularity(JsonNode priceGranularity) {
        try {
            return mapper.mapper().treeToValue(priceGranularity, ExtPriceGranularity.class);
        } catch (JsonProcessingException e) {
            throw new PreBidException(String.format("Error decoding bidRequest.prebid.targeting.pricegranularity: %s",
                    e.getMessage()), e);
        }
    }

    /**
     * Creates {@link CacheAsset} for the given cache ID.
     */
    private CacheAsset toCacheAsset(String cacheId) {
        return CacheAsset.of(cacheAssetUrlTemplate.concat(cacheId), cacheId);
    }

    private String integrationFrom(AuctionContext auctionContext) {
        final ExtRequest extRequest = auctionContext.getBidRequest().getExt();
        final ExtRequestPrebid prebid = extRequest == null ? null : extRequest.getPrebid();

        return prebid != null ? prebid.getIntegration() : null;
    }

    private static <T> Set<T> nullIfEmpty(Set<T> set) {
        if (set.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableSet(set);
    }

    private static <K, V> Map<K, V> nullIfEmpty(Map<K, V> map) {
        if (map.isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Creates {@link ExtBidPrebidVideo} from bid extension.
     */
    private ExtBidPrebidVideo getExtBidPrebidVideo(ObjectNode bidExt) {
        final ExtPrebid<ExtBidPrebid, ObjectNode> extPrebid = mapper.mapper()
                .convertValue(bidExt, EXT_PREBID_TYPE_REFERENCE);
        final ExtBidPrebid extBidPrebid = extPrebid != null ? extPrebid.getPrebid() : null;
        return extBidPrebid != null ? extBidPrebid.getVideo() : null;
    }
}
