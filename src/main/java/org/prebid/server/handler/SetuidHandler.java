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
            context.response().setStatusCode(status).end();
            metrics.updateCookieSyncOptoutMetric();
            analyticsReporter.processEvent(SetuidEvent.error(status));
            return;
        }

        final String bidder = context.request().getParam(BIDDER_PARAM);
        if (StringUtils.isBlank(bidder)) {
            respondWithMissingParamMessage(BIDDER_PARAM, context);
            return;
        }

        final String gdpr = context.request().getParam("gdpr");
        final String gdprConsent = context.request().getParam("gdpr_consent");
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
        metrics.updateCookieSyncBadRequestMetric();
        analyticsReporter.processEvent(SetuidEvent.error(status));
    }

    private void handleResult(AsyncResult<GdprResponse> asyncResult, RoutingContext context,
                              UidsCookie uidsCookie, String account, String bidder, String gdprConsent, String ip) {
        final boolean gdprProcessingFailed = asyncResult.failed();
        final GdprResponse gdprResponse = asyncResult.result();
        final boolean allowedCookie = !gdprProcessingFailed && gdprResponse.getVendorsToGdpr().values()
                .iterator().next();

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
            if (uidsAudit == null && StringUtils.isBlank(account)) {
                respondWithMissingParamMessage(ACCOUNT_PARAM, context);
                return;
            }

            final Cookie uidsAuditCookie;
            try {
                if (uidsAudit != null) {
                    uidsAuditCookie = uidsAuditCookieService.updateUidsAuditCookie(context, gdprConsent, uidsAudit);
                } else {
                    final String uid = context.request().getParam("uid");
                    uidsAuditCookie = uidsAuditCookieService
                            .createUidsAuditCookie(context, uid, account, gdprConsent, gdprResponse.getCountry(), ip);
                }
            } catch (PreBidException ex) {
                final String errorMessage = String.format("Error occurred on audit cookie creation, "
                        + "uid cookie will not be set without audit: %s", ex.getMessage());
                logger.warn(errorMessage);
                context.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end(errorMessage);
                return;
            }

            respondWithCookie(context, bidder, uidsCookie, uidsAuditCookie);
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

    private void respondWithCookie(RoutingContext context, String bidder, UidsCookie uidsCookie,
                                   Cookie uidsAuditCookie) {
        final String uid = context.request().getParam("uid");

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
            metrics.updateCookieSyncSetsMetric(bidder);
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
        metrics.updateCookieSyncGdprPreventMetric(bidder);
        analyticsReporter.processEvent(SetuidEvent.error(status));
    }
}
