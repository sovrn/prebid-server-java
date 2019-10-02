package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.geolocation.GeoLocationService;
import org.prebid.server.geolocation.model.GeoInfo;
import org.prebid.server.vertx.CircuitBreaker;

import java.net.InetAddress;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class CircuitBreakerSecuredNetAcuityGeoLocationService implements GeoLocationService {

    private static final Logger logger = LoggerFactory.getLogger(
            CircuitBreakerSecuredNetAcuityGeoLocationService.class);

    private final Function<InetAddress, CircuitBreaker> circuitBreakerCreator;
    private final Map<InetAddress, CircuitBreaker> circuitBreakerByAddress = new ConcurrentHashMap<>();

    private final NetAcuityServerAddressProvider netAcuityServerAddressProvider;
    private final NetAcuityGeoLocationService netAcuityGeoLocationService;

    public CircuitBreakerSecuredNetAcuityGeoLocationService(
            NetAcuityGeoLocationService netAcuityGeoLocationService,
            NetAcuityServerAddressProvider netAcuityServerAddressProvider,
            Vertx vertx, int openingThreshold, long openingIntervalMs, long closingIntervalMs, Clock clock) {

        this.netAcuityServerAddressProvider = Objects.requireNonNull(netAcuityServerAddressProvider);
        this.netAcuityGeoLocationService = Objects.requireNonNull(netAcuityGeoLocationService);

        circuitBreakerCreator = server -> new CircuitBreaker(
                "net-acuity-circuit-breaker-" + server.getHostAddress(), Objects.requireNonNull(vertx),
                openingThreshold, openingIntervalMs, closingIntervalMs, Objects.requireNonNull(clock))
                .openHandler(ignored -> circuitOpened(server))
                .halfOpenHandler(ignored -> circuitHalfOpened(server))
                .closeHandler(ignored -> circuitClosed());
    }

    private void circuitOpened(InetAddress server) {
        netAcuityServerAddressProvider.removeServerAddress(server);
        logger.warn("NetAcuity service is unavailable, circuit opened.");
    }

    private void circuitHalfOpened(InetAddress server) {
        netAcuityServerAddressProvider.addServerAddress(server);
        logger.warn("NetAcuity service is ready to try again, circuit half-opened.");
    }

    private void circuitClosed() {
        logger.warn("NetAcuity service becomes working, circuit closed.");
    }

    @Override
    public Future<GeoInfo> lookup(String ip, Timeout timeout) {
        final InetAddress serverAddress;
        try {
            serverAddress = netAcuityServerAddressProvider.getServerAddress();
        } catch (PreBidException e) {
            return Future.failedFuture(e);
        }
        return circuitBreakerByAddress.computeIfAbsent(serverAddress, circuitBreakerCreator)
                .execute(future -> netAcuityGeoLocationService.lookup(ip, timeout, serverAddress).setHandler(future));
    }
}
