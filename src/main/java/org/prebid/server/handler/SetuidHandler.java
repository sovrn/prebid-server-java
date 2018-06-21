package org.prebid.server.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Cookie;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.analytics.AnalyticsReporter;
import org.prebid.server.analytics.model.SetuidEvent;
import org.prebid.server.cookie.UidsCookie;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.gdpr.GdprService;
import org.prebid.server.gdpr.model.GdprPurpose;
import org.prebid.server.gdpr.model.GdprResponse;
import org.prebid.server.metric.MetricName;
import org.prebid.server.metric.Metrics;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.util.HttpUtil;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

public class SetuidHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(SetuidHandler.class);
    private static final Set<GdprPurpose> GDPR_PURPOSES =
            Collections.unmodifiableSet(EnumSet.of(GdprPurpose.informationStorageAndAccess));
    private static final String BIDDER_PARAM = "bidder";
    private static final String ACCOUNT_ID_PARAM = "account_id";

    private final boolean enableCookie;
    private final UidsCookieService uidsCookieService;
    private final UidsAuditCookieService uidsAuditCookieService;
    private final GdprService gdprService;
    private final Set<Integer> gdprVendorIds;
    private final boolean useGeoLocation;
    private final AnalyticsReporter analyticsReporter;
    private final Metrics metrics;

    public SetuidHandler(boolean enableCookie, UidsCookieService uidsCookieService,
                         UidsAuditCookieService uidsAuditCookieService, GdprService gdprService,
                         Integer gdprHostVendorId, boolean useGeoLocation, AnalyticsReporter analyticsReporter,
                         Metrics metrics) {
        this.enableCookie = enableCookie;
        this.uidsCookieService = Objects.requireNonNull(uidsCookieService);
        this.uidsAuditCookieService = uidsAuditCookieService;
        this.gdprService = Objects.requireNonNull(gdprService);
        this.gdprVendorIds = Collections.singleton(gdprHostVendorId);
        this.useGeoLocation = useGeoLocation;
        this.analyticsReporter = Objects.requireNonNull(analyticsReporter);
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public void handle(RoutingContext context) {
        if (!enableCookie) {
            context.response().setStatusCode(HttpResponseStatus.NO_CONTENT.code()).end();
            return;
        }

        final UidsCookie uidsCookie = uidsCookieService.parseFromRequest(context);
        if (!uidsCookie.allowsSync()) {
            final int status = HttpResponseStatus.UNAUTHORIZED.code();
            context.response().setStatusCode(status).end();
            metrics.cookieSync().incCounter(MetricName.opt_outs);
            analyticsReporter.processEvent(SetuidEvent.error(status));
            return;
        }

        final String accountId = context.request().getParam(ACCOUNT_ID_PARAM);
        if (StringUtils.isBlank(accountId)) {
            respondWithMissingParamMessage(ACCOUNT_ID_PARAM, context);
            return;
        }

        final String bidder = context.request().getParam(BIDDER_PARAM);
        if (StringUtils.isBlank(bidder)) {
            respondWithMissingParamMessage(BIDDER_PARAM, context);
            return;
        }

        final String gdpr = context.request().getParam("gdpr");
        final String gdprConsent = context.request().getParam("gdpr_consent");
        final String ip = useGeoLocation ? HttpUtil.ipFrom(context.request()) : null;
        gdprService.resultByVendor(GDPR_PURPOSES, gdprVendorIds, gdpr, gdprConsent, context, ip)
                .setHandler(asyncResult -> handleResult(asyncResult, context, uidsCookie, accountId, bidder,
                        gdprConsent, ip));
    }

    private void respondWithMissingParamMessage(String param, RoutingContext context) {
        final int status = HttpResponseStatus.BAD_REQUEST.code();
        context.response().setStatusCode(status).end(String.format("\"%s\" query param is required", param));
        metrics.cookieSync().incCounter(MetricName.bad_requests);
        analyticsReporter.processEvent(SetuidEvent.error(status));
    }

    private void handleResult(AsyncResult<GdprResponse> asyncResult, RoutingContext context,
                              UidsCookie uidsCookie, String accountId, String bidder, String gdprConsent, String ip) {
        final boolean gdprProcessingFailed = asyncResult.failed();
        final GdprResponse gdprResponse = asyncResult.result();
        final boolean allowedCookie = !gdprProcessingFailed && gdprResponse.getVendorsToGdpr().values()
                .iterator().next();

        if (allowedCookie) {
            respondWithCookie(context, accountId, bidder, uidsCookie, gdprConsent, gdprResponse.getCountry(), ip);
        } else {
            final int status;
            final String body;

            if (gdprProcessingFailed) {
                status = HttpResponseStatus.BAD_REQUEST.code();
                body = asyncResult.cause().getMessage();
            } else {
                status = HttpResponseStatus.OK.code();
                body = "The gdpr_consent param prevents cookies from being saved";
            }

            respondWithoutCookie(context, status, body, bidder);
        }
    }

    private void respondWithCookie(RoutingContext context, String accountId, String bidder, UidsCookie uidsCookie,
                                   String gdprConsent, String country, String ip) {
        final String uid = context.request().getParam("uid");
        final Cookie uidsAuditCookie;
        try {
            uidsAuditCookie = uidsAuditCookieService
                    .createUidsAuditCookie(context, uid, accountId, gdprConsent, country, ip);
        } catch (PreBidException ex) {
            final String errorMessage = String.format("Error occurred on audit cookie creation, uid cookie will not be"
                    + " set without audit: %s", ex.getMessage());
            logger.warn(errorMessage);
            context.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(errorMessage);
            return;
        }

        final UidsCookie updatedUidsCookie;
        boolean successfullyUpdated = false;

        if (StringUtils.isBlank(uid)) {
            updatedUidsCookie = uidsCookie.deleteUid(bidder);
        } else if (UidsCookie.isFacebookSentinel(bidder, uid)) {
            // At the moment, Facebook calls /setuid with a UID of 0 if the user isn't logged into Facebook.
            // They shouldn't be sending us a sentinel value... but since they are, we're refusing to save that ID.
            updatedUidsCookie = uidsCookie;
        } else {
            updatedUidsCookie = uidsCookie.updateUid(bidder, uid);
            successfullyUpdated = true;
            metrics.cookieSync().forBidder(bidder).incCounter(MetricName.sets);
        }

        final Cookie cookie = uidsCookieService.toCookie(updatedUidsCookie);
        context.addCookie(cookie).addCookie(uidsAuditCookie).response().end();

        analyticsReporter.processEvent(SetuidEvent.builder()
                .status(HttpResponseStatus.OK.code())
                .bidder(bidder)
                .uid(uid)
                .success(successfullyUpdated)
                .build());
    }

    private void respondWithoutCookie(RoutingContext context, int status, String body, String bidder) {
        context.response().setStatusCode(status).end(body);
        metrics.cookieSync().forBidder(bidder).incCounter(MetricName.gdpr_prevent);
        analyticsReporter.processEvent(SetuidEvent.error(status));
    }
}
