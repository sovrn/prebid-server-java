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
import org.prebid.server.exception.InvalidRequestException;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.TimeoutFactory;
import org.prebid.server.gdpr.GdprService;
import org.prebid.server.gdpr.model.GdprPurpose;
import org.prebid.server.gdpr.model.GdprResponse;
import org.prebid.server.metric.Metrics;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.rubicon.audit.proto.UidAudit;
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
    private static final String GDPR_PARAM = "gdpr";
    private static final String GDPR_CONSENT_PARAM = "gdpr_consent";
    private static final String UID_PARAM = "uid";
    private static final String FORMAT_PARAM = "format";
    private static final String IMG_FORMAT_PARAM = "img";
    private static final String PIXEL_FILE_PATH = "static/tracking-pixel.png";
    private static final String ACCOUNT_PARAM = "account";

    private final long defaultTimeout;
    private final UidsCookieService uidsCookieService;
    private final GdprService gdprService;
    private final Set<Integer> gdprVendorIds;
    private final boolean useGeoLocation;
    private final AnalyticsReporter analyticsReporter;
    private final Metrics metrics;
    private final TimeoutFactory timeoutFactory;
    private final boolean enableCookie;
    private final UidsAuditCookieService uidsAuditCookieService;

    public SetuidHandler(long defaultTimeout, UidsCookieService uidsCookieService, GdprService gdprService,
                         Integer gdprHostVendorId, boolean useGeoLocation, AnalyticsReporter analyticsReporter,
                         Metrics metrics, TimeoutFactory timeoutFactory, boolean enableCookie,
                         UidsAuditCookieService uidsAuditCookieService) {
        this.defaultTimeout = defaultTimeout;
        this.uidsCookieService = Objects.requireNonNull(uidsCookieService);
        this.gdprService = Objects.requireNonNull(gdprService);
        this.gdprVendorIds = Collections.singleton(gdprHostVendorId);
        this.useGeoLocation = useGeoLocation;
        this.analyticsReporter = Objects.requireNonNull(analyticsReporter);
        this.metrics = Objects.requireNonNull(metrics);
        this.timeoutFactory = Objects.requireNonNull(timeoutFactory);
        this.enableCookie = enableCookie;
        this.uidsAuditCookieService = uidsAuditCookieService;
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
            respondWith(context, status, null);
            metrics.updateUserSyncOptoutMetric();
            analyticsReporter.processEvent(SetuidEvent.error(status));
            return;
        }

        final String bidder = context.request().getParam(BIDDER_PARAM);
        if (StringUtils.isBlank(bidder)) {
            final int status = HttpResponseStatus.BAD_REQUEST.code();
            respondWith(context, status, "\"bidder\" query param is required");
            metrics.updateUserSyncBadRequestMetric();
            analyticsReporter.processEvent(SetuidEvent.error(status));
            return;
        }

        final String gdpr = context.request().getParam(GDPR_PARAM);
        final String gdprConsent = context.request().getParam(GDPR_CONSENT_PARAM);
        final String account = context.request().getParam(ACCOUNT_PARAM);
        final String ip = useGeoLocation ? HttpUtil.ipFrom(context.request()) : null;
        gdprService.resultByVendor(GDPR_PURPOSES, gdprVendorIds, gdpr, gdprConsent, ip,
                timeoutFactory.create(defaultTimeout), context)
                .setHandler(asyncResult -> handleResult(asyncResult, context, uidsCookie, account, bidder,
                        gdprConsent, ip));
    }

    private void respondWithMissingParamMessage(String param, RoutingContext context) {
        final int status = HttpResponseStatus.BAD_REQUEST.code();
        context.response().setStatusCode(status).end(String.format("\"%s\" query param is required", param));
        metrics.updateUserSyncBadRequestMetric();
        analyticsReporter.processEvent(SetuidEvent.error(status));
    }

    private void handleResult(AsyncResult<GdprResponse> asyncResult, RoutingContext context,
                              UidsCookie uidsCookie, String account, String bidder, String gdprConsent, String ip) {

        final boolean gdprProcessingFailed = asyncResult.failed();
        final GdprResponse gdprResponse = !gdprProcessingFailed ? asyncResult.result() : null;

        // allow cookie only if user is not in GDPR scope or vendor passes GDPR check
        final boolean allowedCookie = gdprResponse != null
                && (!gdprResponse.isUserInGdprScope() || gdprResponse.getVendorsToGdpr().values().iterator().next());

        if (allowedCookie) {
            final UidAudit uidsAudit;
            try {
                uidsAudit = uidsAuditCookieService.getUidsAudit(context);
            } catch (PreBidException e) {
                final String errorMessage = String.format("Error retrieving of audit cookie: %s", e.getMessage());
                logger.warn(errorMessage);
                context.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(errorMessage);
                return;
            }

            // check do we really need account parameter
            if (uidsAudit == null && StringUtils.isBlank(account) && gdprResponse.isUserInGdprScope()) {
                respondWithMissingParamMessage(ACCOUNT_PARAM, context);
                return;
            }

            final Cookie uidsAuditCookie;
            try {
                if (uidsAudit != null) {
                    uidsAuditCookie = uidsAuditCookieService.updateUidsAuditCookie(context, gdprConsent, uidsAudit);
                } else if (StringUtils.isNotBlank(account)) {
                    final String uid = context.request().getParam(UID_PARAM);
                    uidsAuditCookie = uidsAuditCookieService
                            .createUidsAuditCookie(context, uid, account, gdprConsent, gdprResponse.getCountry(), ip);
                } else {
                    uidsAuditCookie = null; // user is not in GDPR scope, so uids-audit cookie is not required
                }
            } catch (PreBidException e) {
                final String errorMessage = String.format("Error occurred on audit cookie creation, "
                        + "uid cookie will not be set without audit: %s", e.getMessage());
                logger.warn(errorMessage);
                context.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(errorMessage);
                return;
            }

            respondWithCookie(context, bidder, uidsCookie, uidsAuditCookie);
        } else {
            final int status;
            final String body;

            if (gdprProcessingFailed) {
                final Throwable exception = asyncResult.cause();
                if (exception instanceof InvalidRequestException) {
                    status = HttpResponseStatus.BAD_REQUEST.code();
                    body = String.format("GDPR processing failed with error: %s", exception.getMessage());
                } else {
                    status = HttpResponseStatus.INTERNAL_SERVER_ERROR.code();
                    body = "Unexpected GDPR processing error";
                    logger.warn(body, exception);
                }
            } else {
                status = HttpResponseStatus.OK.code();
                body = "The gdpr_consent param prevents cookies from being saved";
            }

            respondWithoutCookie(context, status, body, bidder);
        }
    }

    private void respondWithCookie(RoutingContext context, String bidder, UidsCookie uidsCookie,
                                   Cookie uidsAuditCookie) {
        final String uid = context.request().getParam(UID_PARAM);
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
            metrics.updateUserSyncSetsMetric(bidder);
        }

        final Cookie cookie = uidsCookieService.toCookie(updatedUidsCookie);
        addCookie(context, cookie);

        if (uidsAuditCookie != null) {
            addCookie(context, uidsAuditCookie);
        }

        final int status = HttpResponseStatus.OK.code();

        // Send pixel file to response if "format=img"
        final String format = context.request().getParam(FORMAT_PARAM);
        if (StringUtils.equals(format, IMG_FORMAT_PARAM)) {
            context.response().sendFile(PIXEL_FILE_PATH);
        } else {
            respondWith(context, status, null);
        }

        analyticsReporter.processEvent(SetuidEvent.builder()
                .status(status)
                .bidder(bidder)
                .uid(uid)
                .success(successfullyUpdated)
                .build());
    }

    private void addCookie(RoutingContext context, Cookie cookie) {
        context.response().headers().add(HttpUtil.SET_COOKIE_HEADER, HttpUtil.toSetCookieHeaderValue(cookie));
    }

    private void respondWithoutCookie(RoutingContext context, int status, String body, String bidder) {
        respondWith(context, status, body);
        metrics.updateUserSyncGdprPreventMetric(bidder);
        analyticsReporter.processEvent(SetuidEvent.error(status));
    }

    private static void respondWith(RoutingContext context, int status, String body) {
        // don't send the response if client has gone
        if (context.response().closed()) {
            logger.warn("The client already closed connection, response will be skipped");
            return;
        }

        context.response().setStatusCode(status);
        if (body != null) {
            context.response().end(body);
        } else {
            context.response().end();
        }
    }
}
