package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import net.digitalenvoy.netacuity.api.DbAccessor;
import net.digitalenvoy.netacuity.api.DbAccessorFactory;
import net.digitalenvoy.netacuity.api.PulseQuery;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.geolocation.GeoLocationService;
import org.prebid.server.geolocation.model.GeoInfo;
import org.prebid.server.metric.MetricName;
import org.prebid.server.metric.Metrics;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class NetAcuityGeoLocationService implements GeoLocationService {

    private static final Logger logger = LoggerFactory.getLogger(NetAcuityGeoLocationService.class);

    private static final String VENDOR = "netacuity";

    private static final int API_ID = 1;

    private final Vertx vertx;
    private final Supplier<InetAddress> serverAddress;
    private final Clock clock;
    private final Metrics metrics;

    public NetAcuityGeoLocationService(Vertx vertx,
                                       Supplier<InetAddress> serverAddress,
                                       Clock clock,
                                       Metrics metrics) {
        this.vertx = Objects.requireNonNull(vertx);
        this.serverAddress = Objects.requireNonNull(serverAddress);
        this.clock = Objects.requireNonNull(clock);
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public Future<GeoInfo> lookup(String ip, Timeout timeout) {
        final InetAddress server;
        try {
            server = serverAddress.get();
        } catch (PreBidException e) {
            return Future.failedFuture(e);
        }
        return lookup(ip, timeout, server);
    }

    /**
     * A work-around overloaded method for {@link CircuitBreakerSecuredNetAcuityGeoLocationService}
     * The idea is to use the same serverAddress variable while getting these addresses randomly.
     */
    public Future<GeoInfo> lookup(String ip, Timeout timeout, InetAddress serverAddress) {
        final Promise<GeoInfo> promise = Promise.promise();
        vertx.executeBlocking(executeFuture -> doLookup(executeFuture, serverAddress, ip, timeout), false, promise);
        return promise.future();
    }

    private void doLookup(Promise<GeoInfo> promise, InetAddress serverAddress, String ip, Timeout timeout) {
        final long remainingTimeout = timeout.remaining();
        final long startTime = clock.millis();

        if (remainingTimeout <= 0) {
            failWith(new TimeoutException("Timeout has been exceeded"), startTime).setHandler(promise);
            return;
        }

        final InetAddress lookupAddress;
        try {
            lookupAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            final String message = String.format("Invalid IP address to lookup: %s", ip);
            failWith(new PreBidException(message, e), startTime).setHandler(promise);
            return;
        }

        try {
            final DbAccessor dbAccessor = DbAccessorFactory.getAccessor(serverAddress, API_ID,
                    Math.toIntExact(remainingTimeout));

            final PulseQuery query = dbAccessor.query(PulseQuery.class, lookupAddress);
            if (isValidResponse(query)) {
                successWith(query, startTime).setHandler(promise);
            } else {
                fallbackWithEmpty(ip, startTime).setHandler(promise);
            }
        } catch (ArithmeticException | IllegalArgumentException | IOException e) {
            failWith(new PreBidException("Geo location lookup failed", e), startTime).setHandler(promise);
        }
    }

    /**
     * In case NAC cannot determine geo info by IP it responds with strange result where country = ** or similar.
     * So, this method guarantees result is valid.
     */
    private static boolean isValidResponse(PulseQuery query) {
        final String country = query.getTwoLetterCountry();
        return StringUtils.length(country) == 2 && StringUtils.isAlpha(country);
    }

    private Future<GeoInfo> successWith(PulseQuery query, long startTime) {
        metrics.updateRequestTimeMetric(MetricName.geolocation_request_time, responseTime(startTime));

        return Future.succeededFuture(GeoInfo.builder()
                .vendor(VENDOR)
                .country(query.getTwoLetterCountry())
                .region(query.getRegion())
                .regionCode(query.getRegionCode())
                .city(query.getCity())
                .metroNielsen(query.getMetroCode())
                .zip(query.getPostalCode())
                .connectionSpeed(query.getConnectionSpeed())
                .lat(query.getLatitude())
                .lon(query.getLongitude())
                .timeZone(getTimeZone(query))
                .build());
    }

    private static ZoneId getTimeZone(PulseQuery query) {
        final String originalTimeZone = query.getTimezoneName();
        final String capitalizedTimeZone = WordUtils.capitalizeFully(originalTimeZone, '/', '_');
        try {
            return Objects.equals("?", originalTimeZone) ? null : ZoneId.of(capitalizedTimeZone);
        } catch (DateTimeException e) {
            logger.info("Unrecognized time zone from NetAcuity. "
                            + "Original designation: [{0}], capitalized designation: [{1}]",
                    originalTimeZone, capitalizedTimeZone);
            return null;
        }
    }

    private Future<GeoInfo> fallbackWithEmpty(String ip, long startTime) {
        metrics.updateRequestTimeMetric(MetricName.geolocation_request_time, responseTime(startTime));
        logger.warn("Cannot determine geo info for IP: {0}", ip);

        return Future.succeededFuture(GeoInfo.builder().vendor(VENDOR).build());
    }

    private Future<GeoInfo> failWith(Throwable exception, long startTime) {
        metrics.updateRequestTimeMetric(MetricName.geolocation_request_time, responseTime(startTime));
        logger.warn("NetAcuity geo location service error: {0}", exception.getMessage());

        return Future.failedFuture(exception);
    }

    /**
     * Calculates execution time since the given start time.
     */
    private int responseTime(long startTime) {
        return Math.toIntExact(clock.millis() - startTime);
    }
}
