package org.prebid.server.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.deals.DeliveryProgressService;
import org.prebid.server.deals.proto.report.LineItemStatusReport;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.util.HttpUtil;

import java.time.ZonedDateTime;
import java.util.Objects;

public class LineItemStatusHandler implements Handler<RoutingContext> {

    private static final Logger logger = LoggerFactory.getLogger(LineItemStatusHandler.class);

    private static final String ID_PARAM = "id";
    private static final String PG_SIM_TIMESTAMP = "pg-sim-timestamp";

    private final DeliveryProgressService deliveryProgressService;
    private final JacksonMapper mapper;

    public LineItemStatusHandler(DeliveryProgressService deliveryProgressService, JacksonMapper mapper) {
        this.deliveryProgressService = Objects.requireNonNull(deliveryProgressService);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public void handle(RoutingContext context) {
        context.response()
                .exceptionHandler(LineItemStatusHandler::handleResponseException);

        final String lineItemId = lineItemIdFrom(context);
        if (StringUtils.isEmpty(lineItemId)) {
            final String body = String.format("%s parameter is required", ID_PARAM);
            HttpUtil.respondWith(context, HttpResponseStatus.BAD_REQUEST, body);
            return;
        }

        try {
            final ZonedDateTime time = HttpUtil.getDateFromHeader(context.request().headers(), PG_SIM_TIMESTAMP);
            final LineItemStatusReport report = deliveryProgressService.getLineItemStatusReport(lineItemId, time);

            HttpUtil.headers().forEach(entry -> context.response().putHeader(entry.getKey(), entry.getValue()));
            HttpUtil.respondWith(context, HttpResponseStatus.OK, mapper.encode(report));
        } catch (PreBidException e) {
            HttpUtil.respondWith(context, HttpResponseStatus.BAD_REQUEST, e.getMessage());
        } catch (Exception e) {
            HttpUtil.respondWith(context, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private static String lineItemIdFrom(RoutingContext context) {
        return context.request().getParam(ID_PARAM);
    }

    private static void handleResponseException(Throwable exception) {
        logger.warn("Failed to send line item status response: {0}", exception.getMessage());
    }
}
