package org.prebid.server.auction.requestfactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Device;
import com.iab.openrtb.request.Geo;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Site;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.auction.TimeoutResolver;
import org.prebid.server.auction.model.AuctionContext;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.deals.DealsProcessor;
import org.prebid.server.deals.model.DeepDebugLog;
import org.prebid.server.deals.model.TxnLog;
import org.prebid.server.exception.BlacklistedAccountException;
import org.prebid.server.exception.InvalidRequestException;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.exception.UnauthorizedAccountException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.execution.TimeoutFactory;
import org.prebid.server.geolocation.model.GeoInfo;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.log.ConditionalLogger;
import org.prebid.server.metric.MetricName;
import org.prebid.server.privacy.model.PrivacyContext;
import org.prebid.server.proto.openrtb.ext.request.ExtImpPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtPublisher;
import org.prebid.server.proto.openrtb.ext.request.ExtPublisherPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtStoredRequest;
import org.prebid.server.proto.openrtb.ext.request.rubicon.ExtImpRubicon;
import org.prebid.server.settings.ApplicationSettings;
import org.prebid.server.settings.model.Account;
import org.prebid.server.settings.model.AccountStatus;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.util.StreamUtil;
import org.prebid.server.validation.RequestValidator;
import org.prebid.server.validation.model.ValidationResult;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Ortb2RequestFactory {

    private static final Logger logger = LoggerFactory.getLogger(Ortb2RequestFactory.class);

    private static final ConditionalLogger EMPTY_ACCOUNT_LOGGER = new ConditionalLogger("empty_account", logger);
    private static final ConditionalLogger UNKNOWN_ACCOUNT_LOGGER = new ConditionalLogger("unknown_account", logger);

    private static final String RUBICON_BIDDER = "rubicon";
    private static final String PREBID_EXT = "prebid";
    private static final String BIDDER_EXT = "bidder";

    private final boolean enforceValidAccount;
    private final List<String> blacklistedAccounts;
    private final UidsCookieService uidsCookieService;
    private final RequestValidator requestValidator;
    private final TimeoutFactory timeoutFactory;
    private final ApplicationSettings applicationSettings;
    private final DealsProcessor dealsProcessor;
    private final TimeoutResolver timeoutResolver;
    private final Clock clock;
    private final JacksonMapper mapper;

    public Ortb2RequestFactory(boolean enforceValidAccount,
                               List<String> blacklistedAccounts,
                               UidsCookieService uidsCookieService,
                               RequestValidator requestValidator,
                               TimeoutResolver timeoutResolver,
                               TimeoutFactory timeoutFactory,
                               ApplicationSettings applicationSettings,
                               DealsProcessor dealsProcessor,
                               Clock clock,
                               JacksonMapper mapper) {

        this.enforceValidAccount = enforceValidAccount;
        this.blacklistedAccounts = Objects.requireNonNull(blacklistedAccounts);
        this.uidsCookieService = Objects.requireNonNull(uidsCookieService);
        this.requestValidator = Objects.requireNonNull(requestValidator);
        this.timeoutResolver = Objects.requireNonNull(timeoutResolver);
        this.timeoutFactory = Objects.requireNonNull(timeoutFactory);
        this.applicationSettings = Objects.requireNonNull(applicationSettings);
        this.dealsProcessor = dealsProcessor;
        this.clock = Objects.requireNonNull(clock);
        this.mapper = Objects.requireNonNull(mapper);
    }

    public Future<AuctionContext> fetchAccountAndCreateAuctionContext(RoutingContext routingContext,
                                                                      BidRequest bidRequest,
                                                                      MetricName requestTypeMetric,
                                                                      long startTime,
                                                                      List<String> errors) {
        final Timeout timeout = timeout(bidRequest, startTime, timeoutResolver);
        return accountFrom(bidRequest, timeout, routingContext)
                .map(account -> AuctionContext.builder()
                        .routingContext(routingContext)
                        .uidsCookie(uidsCookieService.parseFromRequest(routingContext))
                        .bidRequest(bidRequest)
                        .requestTypeMetric(requestTypeMetric)
                        .timeout(timeout)
                        .account(account)
                        .prebidErrors(errors)
                        .debugWarnings(new ArrayList<>())
                        .txnLog(TxnLog.create().accountId(account.getId()))
                        .deepDebugLog(createDeepDebugLog(bidRequest))
                        .debugHttpCalls(new HashMap<>())
                        .build());
    }

    /**
     * Performs thorough validation of fully constructed {@link BidRequest} that is going to be used to hold an auction.
     */
    public BidRequest validateRequest(BidRequest bidRequest) {
        final ValidationResult validationResult = requestValidator.validate(bidRequest);
        if (validationResult.hasErrors()) {
            throw new InvalidRequestException(validationResult.getErrors());
        }
        return bidRequest;
    }

    public BidRequest enrichBidRequestWithAccountAndPrivacyData(BidRequest bidRequest,
                                                                Account account,
                                                                PrivacyContext privacyContext) {

        final ExtRequest requestExt = bidRequest.getExt();
        final ExtRequest enrichedRequestExt = enrichExtRequest(requestExt, account);

        final Device device = bidRequest.getDevice();
        final Device enrichedDevice = enrichDevice(device, privacyContext);

        final Site site = bidRequest.getSite();
        final Site enrichedSite = enrichSiteWithAccountId(site, account);

        final App app = bidRequest.getApp();
        final App enrichedApp = enrichAppWithAccountId(app, account);

        if (enrichedRequestExt != null || enrichedDevice != null
                || enrichedSite != null || enrichedApp != null) {
            return bidRequest.toBuilder()
                    .ext(ObjectUtils.defaultIfNull(enrichedRequestExt, requestExt))
                    .device(ObjectUtils.defaultIfNull(enrichedDevice, device))
                    .site(ObjectUtils.defaultIfNull(enrichedSite, site))
                    .app(ObjectUtils.defaultIfNull(enrichedApp, app))
                    .build();
        }

        return bidRequest;
    }

    public Future<AuctionContext> populateDealsInfo(AuctionContext auctionContext) {
        return dealsProcessor != null
                ? dealsProcessor.populateDealsInfo(auctionContext)
                : Future.succeededFuture(auctionContext);
    }

    /**
     * Returns {@link Timeout} based on request.tmax and adjustment value of {@link TimeoutResolver}.
     */
    private Timeout timeout(BidRequest bidRequest, long startTime, TimeoutResolver timeoutResolver) {
        final long resolvedRequestTimeout = timeoutResolver.resolve(bidRequest.getTmax());
        final long timeout = timeoutResolver.adjustTimeout(resolvedRequestTimeout);
        return timeoutFactory.create(startTime, timeout);
    }

    /**
     * Returns {@link Account} fetched by {@link ApplicationSettings}.
     */
    private Future<Account> accountFrom(BidRequest bidRequest, Timeout timeout, RoutingContext routingContext) {
        final String accountId = accountIdFrom(bidRequest);
        final boolean isAccountIdBlank = StringUtils.isBlank(accountId);

        if (CollectionUtils.isNotEmpty(blacklistedAccounts)
                && !isAccountIdBlank
                && blacklistedAccounts.contains(accountId)) {
            return Future.failedFuture(new BlacklistedAccountException(
                    String.format("Prebid-server has blacklisted Account ID: %s, please "
                            + "reach out to the prebid server host.", accountId)));
        }

        return isAccountIdBlank
                ? responseForEmptyAccount(routingContext)
                : applicationSettings.getAccountById(accountId, timeout)
                .compose(this::ensureAccountActive,
                        exception -> accountFallback(exception, accountId, routingContext));
    }

    /**
     * Extracts publisher id either from {@link BidRequest}.app.publisher or {@link BidRequest}.site.publisher.
     * If neither is present returns empty string.
     */
    private String accountIdFrom(BidRequest bidRequest) {
        final App app = bidRequest.getApp();
        final Publisher appPublisher = app != null ? app.getPublisher() : null;
        final Site site = bidRequest.getSite();
        final Publisher sitePublisher = site != null ? site.getPublisher() : null;

        final Publisher publisher = ObjectUtils.defaultIfNull(appPublisher, sitePublisher);
        final String publisherId = publisher != null ? resolvePublisherId(publisher) : null;
        if (accountIsValidNumber(publisherId)) {
            return publisherId;
        }

        final String storedRequestAccountId = accountFromExtPrebidStoredRequestId(bidRequest);
        if (accountIsValidNumber(storedRequestAccountId)) {
            return storedRequestAccountId;
        }

        final String rubiconAccountId = accountFromImpExtRubiconAccountId(bidRequest);
        if (StringUtils.isNotEmpty(rubiconAccountId)) {
            return rubiconAccountId;
        }

        final String impStoredRequestAccountId = accountFromImpExtPrebidStoredRequestId(bidRequest);
        if (accountIsValidNumber(impStoredRequestAccountId)) {
            return impStoredRequestAccountId;
        }

        return StringUtils.EMPTY;
    }

    /**
     * Resolves what value should be used as a publisher id - either taken from publisher.ext.parentAccount
     * or publisher.id in this respective priority.
     */
    private String resolvePublisherId(Publisher publisher) {
        final String parentAccountId = parentAccountIdFromExtPublisher(publisher.getExt());
        return ObjectUtils.defaultIfNull(parentAccountId, publisher.getId());
    }

    /**
     * Parses publisher.ext and returns parentAccount value from it. Returns null if any parsing error occurs.
     */
    private String parentAccountIdFromExtPublisher(ExtPublisher extPublisher) {
        final ExtPublisherPrebid extPublisherPrebid = extPublisher != null ? extPublisher.getPrebid() : null;
        return extPublisherPrebid != null ? StringUtils.stripToNull(extPublisherPrebid.getParentAccount()) : null;
    }

    private Future<Account> responseForEmptyAccount(RoutingContext routingContext) {
        EMPTY_ACCOUNT_LOGGER.warn(accountErrorMessage("Account not specified", routingContext), 100);
        return responseForUnknownAccount(StringUtils.EMPTY);
    }

    private static String accountErrorMessage(String message, RoutingContext routingContext) {
        final HttpServerRequest request = routingContext.request();
        return String.format(
                "%s, Url: %s and Referer: %s",
                message,
                request.absoluteURI(),
                request.headers().get(HttpUtil.REFERER_HEADER));
    }

    private Future<Account> accountFallback(Throwable exception,
                                            String accountId,
                                            RoutingContext routingContext) {

        if (exception instanceof PreBidException) {
            UNKNOWN_ACCOUNT_LOGGER.warn(accountErrorMessage(exception.getMessage(), routingContext), 100);
        } else {
            logger.warn("Error occurred while fetching account: {0}", exception.getMessage());
            logger.debug("Error occurred while fetching account", exception);
        }

        // hide all errors occurred while fetching account
        return responseForUnknownAccount(accountId);
    }

    private String accountFromExtPrebidStoredRequestId(BidRequest bidRequest) {
        final ExtRequest extRequest = bidRequest.getExt();
        if (extRequest == null) {
            return null;
        }
        final ExtRequestPrebid extRequestPrebid = getIfNotNull(extRequest, ExtRequest::getPrebid);
        final ExtStoredRequest extStoredRequest = getIfNotNull(extRequestPrebid, ExtRequestPrebid::getStoredrequest);
        return extStoredRequest != null ? parseAccountFromStoredRequest(extStoredRequest) : null;
    }

    /**
     * Checks request impression extensions whether they have a rubicon extension, picks first and
     * takes account ID from it. If none is present - returns null.
     */
    private String accountFromImpExtRubiconAccountId(BidRequest bidRequest) {
        final List<Imp> imps = bidRequest.getImp();

        return CollectionUtils.isEmpty(imps) ? null : imps.stream()
                .map(imp -> rubiconParams(imp, rubiconAliases(bidRequest.getExt())))
                .filter(Objects::nonNull)
                .map(this::extImpRubiconOrNull)
                .filter(Objects::nonNull)
                .map(ExtImpRubicon::getAccountId)
                .filter(Objects::nonNull)
                .map(Objects::toString)
                .findFirst()
                .orElse(null);
    }

    /**
     * Returns list of aliases for Rubicon bidder or empty if not defined.
     */
    private Set<String> rubiconAliases(ExtRequest ext) {
        final ExtRequestPrebid prebid = getIfNotNull(ext, ExtRequest::getPrebid);
        final Map<String, String> aliases = getIfNotNullOrDefault(prebid, ExtRequestPrebid::getAliases,
                Collections.emptyMap());

        return aliases.entrySet().stream()
                .filter(entry -> Objects.equals(entry.getValue(), RUBICON_BIDDER))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Returns params {@link JsonNode} of rubicon or its aliases.
     */
    private JsonNode rubiconParams(Imp imp, Set<String> aliases) {
        final ObjectNode ext = imp.getExt();
        if (ext == null || ext.size() == 0) {
            return null;
        }

        return Stream.concat(StreamUtil.asStream(ext.fields()), StreamUtil.asStream(bidderNodesFromImp(imp)))
                .filter(bidderNode ->
                        Objects.equals(bidderNode.getKey(), RUBICON_BIDDER) || aliases.contains(bidderNode.getKey()))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElse(null);
    }

    private Iterator<Map.Entry<String, JsonNode>> bidderNodesFromImp(Imp imp) {
        final JsonNode extPrebid = imp.getExt().get(PREBID_EXT);
        final JsonNode extPrebidBidder = isObjectNode(extPrebid) ? extPrebid.get(BIDDER_EXT) : null;

        return isObjectNode(extPrebidBidder) ? extPrebidBidder.fields() : Collections.emptyIterator();
    }

    /**
     * Extracts {@link ExtImpRubicon} from the given {@link JsonNode}.
     */
    private ExtImpRubicon extImpRubiconOrNull(JsonNode extRubicon) {
        try {
            return mapper.mapper().convertValue(extRubicon, ExtImpRubicon.class);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private String accountFromImpExtPrebidStoredRequestId(BidRequest bidRequest) {
        final List<Imp> imps = bidRequest.getImp();
        return CollectionUtils.isEmpty(imps) ? null : imps.stream()
                .map(Imp::getExt)
                .filter(Objects::nonNull)
                .map(ext -> ext.get(PREBID_EXT))
                .filter(Objects::nonNull)
                .map(this::extImpPrebidOrNull)
                .filter(Objects::nonNull)
                .map(ExtImpPrebid::getStoredrequest)
                .filter(Objects::nonNull)
                .map(Ortb2RequestFactory::parseAccountFromStoredRequest)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /**
     * Extracts {@link ExtImpPrebid} from the given {@link JsonNode}.
     */
    private ExtImpPrebid extImpPrebidOrNull(JsonNode extPrebid) {
        try {
            return mapper.mapper().convertValue(extPrebid, ExtImpPrebid.class);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String parseAccountFromStoredRequest(ExtStoredRequest storedRequest) {
        final String storedRequestId = storedRequest.getId();
        return StringUtils.isNotEmpty(storedRequestId)
                ? storedRequestId.split("-")[0]
                : null;
    }

    private static boolean accountIsValidNumber(String accountId) {
        if (StringUtils.isNumeric(accountId)) {
            try {
                Integer.parseInt(accountId);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    private Future<Account> responseForUnknownAccount(String accountId) {
        return enforceValidAccount
                ? Future.failedFuture(new UnauthorizedAccountException(
                String.format("Unauthorized account id: %s", accountId), accountId))
                : Future.succeededFuture(Account.empty(accountId));
    }

    private Future<Account> ensureAccountActive(Account account) {
        final String accountId = account.getId();

        return account.getStatus() == AccountStatus.inactive
                ? Future.failedFuture(new UnauthorizedAccountException(
                String.format("Account %s is inactive", accountId), accountId))
                : Future.succeededFuture(account);
    }

    private ExtRequest enrichExtRequest(ExtRequest ext, Account account) {
        final ExtRequestPrebid prebidExt = getIfNotNull(ext, ExtRequest::getPrebid);
        final String integration = getIfNotNull(prebidExt, ExtRequestPrebid::getIntegration);
        final String accountDefaultIntegration = account.getDefaultIntegration();

        if (StringUtils.isBlank(integration) && StringUtils.isNotBlank(accountDefaultIntegration)) {
            final ExtRequestPrebid.ExtRequestPrebidBuilder prebidExtBuilder =
                    prebidExt != null ? prebidExt.toBuilder() : ExtRequestPrebid.builder();

            prebidExtBuilder.integration(accountDefaultIntegration);

            return ExtRequest.of(prebidExtBuilder.build());
        }

        return null;
    }

    private Device enrichDevice(Device device, PrivacyContext privacyContext) {
        final String ipAddress = privacyContext.getIpAddress();
        final String country = getIfNotNull(privacyContext.getTcfContext().getGeoInfo(), GeoInfo::getCountry);

        final String ipAddressInRequest = getIfNotNull(device, Device::getIp);

        final Geo geo = getIfNotNull(device, Device::getGeo);
        final String countryFromRequest = getIfNotNull(geo, Geo::getCountry);

        final boolean shouldUpdateIp = ipAddress != null && !Objects.equals(ipAddressInRequest, ipAddress);
        final boolean shouldUpdateCountry = country != null && !Objects.equals(countryFromRequest, country);

        if (shouldUpdateIp || shouldUpdateCountry) {
            final Device.DeviceBuilder deviceBuilder = device != null ? device.toBuilder() : Device.builder();

            if (shouldUpdateIp) {
                deviceBuilder.ip(ipAddress);
            }

            if (shouldUpdateCountry) {
                final Geo.GeoBuilder geoBuilder = geo != null ? geo.toBuilder() : Geo.builder();
                geoBuilder.country(country);
                deviceBuilder.geo(geoBuilder.build());
            }

            return deviceBuilder.build();
        }

        return null;
    }

    /**
     * Rubicon-fork can fetch account id not only from {@link Site} or {@link App},
     * so need to make sure it is updated in {@link BidRequest}.
     */
    private static Site enrichSiteWithAccountId(Site site, Account account) {
        final String accountId = account.getId();

        if (StringUtils.isNotEmpty(accountId) // ignore fallback or empty account
                && site != null) {
            final Publisher publisher = site.getPublisher();
            final String publisherId = publisher != null ? publisher.getId() : null;
            if (!Objects.equals(publisherId, accountId)) {
                return site.toBuilder()
                        .publisher(updatePublisherId(publisher, accountId))
                        .build();
            }
        }

        return null;
    }

    private static App enrichAppWithAccountId(App app, Account account) {
        final String accountId = account.getId();

        if (StringUtils.isNotEmpty(accountId) // ignore fallback or empty account
                && app != null) {
            final Publisher publisher = app.getPublisher();
            final String publisherId = publisher != null ? publisher.getId() : null;
            if (!Objects.equals(publisherId, accountId)) {
                return app.toBuilder()
                        .publisher(updatePublisherId(publisher, accountId))
                        .build();
            }
        }

        return null;
    }

    private static Publisher updatePublisherId(Publisher publisher, String accountId) {
        return (publisher != null ? publisher.toBuilder() : Publisher.builder())
                .id(accountId)
                .build();
    }

    private DeepDebugLog createDeepDebugLog(BidRequest bidRequest) {
        final ExtRequest ext = bidRequest.getExt();
        return DeepDebugLog.create(ext != null && isDeepDebugEnabled(ext), clock);
    }

    /**
     * Determines deep debug flag from {@link ExtRequest}.
     */
    private static boolean isDeepDebugEnabled(ExtRequest extRequest) {
        final ExtRequestPrebid extRequestPrebid = extRequest != null ? extRequest.getPrebid() : null;
        return extRequestPrebid != null && Objects.equals(extRequestPrebid.getTrace(), 1);
    }

    private static boolean isObjectNode(JsonNode node) {
        return node != null && node.isObject();
    }

    private static <T, R> R getIfNotNullOrDefault(T target, Function<T, R> getter, R defaultValue) {
        return ObjectUtils.defaultIfNull(getIfNotNull(target, getter), defaultValue);
    }

    private static <T, R> R getIfNotNull(T target, Function<T, R> getter) {
        return target != null ? getter.apply(target) : null;
    }
}
