package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import net.digitalenvoy.netacuity.api.DbAccessor;
import net.digitalenvoy.netacuity.api.DbAccessorFactory;
import net.digitalenvoy.netacuity.api.PulseQuery;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.geolocation.GeoLocationService;
import org.prebid.server.geolocation.model.GeoInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class NetAcuityGeoLocationService implements GeoLocationService {

    private static final Logger logger = LoggerFactory.getLogger(NetAcuityGeoLocationService.class);

    private static final Random RANDOM = new Random();
    private static final int API_ID = 1;

    private final Vertx vertx;
    private final List<InetAddress> serverAddresses;

    private NetAcuityGeoLocationService(Vertx vertx, List<InetAddress> serverAddresses) {
        this.vertx = vertx;
        this.serverAddresses = serverAddresses;
    }

    public static NetAcuityGeoLocationService create(Vertx vertx, String server) {
        return new NetAcuityGeoLocationService(Objects.requireNonNull(vertx),
                serverAddressesFrom(Objects.requireNonNull(server)));
    }

    private static List<InetAddress> serverAddressesFrom(String server) {
        final List<InetAddress> serverAddresses = new ArrayList<>();
        for (String host : server.split(",")) {
            final InetAddress serverAddress;
            try {
                serverAddress = InetAddress.getByName(host.trim());
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(String.format("Invalid NetAcuity server address: %s", host), e);
            }
            serverAddresses.add(serverAddress);
        }

        if (serverAddresses.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("No NetAcuity server addresses was specified: %s", server));
        }
        return serverAddresses;
    }

    @Override
    public Future<GeoInfo> lookup(String ip, Timeout timeout) {
        final long remainingTimeout = timeout.remaining();
        if (remainingTimeout <= 0) {
            return failWith(new TimeoutException("Timeout has been exceeded"));
        }

        final InetAddress lookupAddress;
        try {
            lookupAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            return failWith(new PreBidException(String.format("Invalid IP address to lookup: %s", ip), e));
        }

        final Future<GeoInfo> future = Future.future();
        vertx.executeBlocking(executeFuture -> doLookup(executeFuture, lookupAddress, remainingTimeout),
                false, future);
        return future;
    }

    private void doLookup(Future<GeoInfo> executeFuture, InetAddress lookupAddress, long remainingTimeout) {
        final DbAccessor dbAccessor = DbAccessorFactory.getAccessor(serverAddress(), API_ID,
                Math.toIntExact(remainingTimeout));
        try {
            final PulseQuery query = dbAccessor.query(PulseQuery.class, lookupAddress);
            executeFuture.complete(GeoInfo.of(query.getTwoLetterCountry()));
        } catch (IllegalArgumentException | IOException e) {
            failWith(new PreBidException("Geo location lookup failed", e)).setHandler(executeFuture);
        }
    }

    private InetAddress serverAddress() {
        final int size = serverAddresses.size();
        final int index = size > 1 ? RANDOM.nextInt(size) : 0;
        return serverAddresses.get(index);
    }

    private static Future<GeoInfo> failWith(Throwable exception) {
        logger.warn("NetAcuity geo location service error", exception);
        return Future.failedFuture(exception);
    }
}
