package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Future;
import net.digitalenvoy.netacuity.api.DbAccessor;
import net.digitalenvoy.netacuity.api.DbAccessorFactory;
import net.digitalenvoy.netacuity.api.PulseQuery;
import net.digitalenvoy.netacuity.api.Query;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.geolocation.GeoLocationService;
import org.prebid.server.geolocation.model.GeoInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class NetAcuityGeoLocationService implements GeoLocationService {

    private static final int API_ID = 1;

    private final InetAddress serverAddress;

    private NetAcuityGeoLocationService(InetAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    public static NetAcuityGeoLocationService create(String server) {
        final InetAddress serverAddress;
        try {
            serverAddress = InetAddress.getByName(Objects.requireNonNull(server));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(String.format("Invalid NetAcuity server address: %s", server), e);
        }

        return new NetAcuityGeoLocationService(serverAddress);
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

        final DbAccessor dbAccessor = DbAccessorFactory.getAccessor(serverAddress, API_ID,
                Math.toIntExact(remainingTimeout));
        final Query query;
        try {
            query = dbAccessor.query(PulseQuery.class, lookupAddress);
        } catch (IllegalArgumentException | IOException e) {
            return failWith(new PreBidException(String.format("Geo location lookup failed: %s", e.getMessage()), e));
        }

        final String country = ((PulseQuery) query).getTwoLetterCountry();
        return Future.succeededFuture(GeoInfo.of(country));
    }

    private static Future<GeoInfo> failWith(Throwable throwable) {
        return Future.failedFuture(throwable);
    }
}
