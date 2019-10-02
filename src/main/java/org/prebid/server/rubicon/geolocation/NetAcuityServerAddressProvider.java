package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Vertx;
import org.prebid.server.exception.PreBidException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class NetAcuityServerAddressProvider {

    private static final Random RANDOM = new Random();
    /**
     * A period of 1 hour (value in ms) that would be used to refresh active servers list by assigning all servers
     * back to it. The intent is to reuse faulty servers by allowing Circuit Breaker to check whether the servers that
     * were excluded are available again and if they are - make use of them.
     */
    private static final long REFRESH_PERIOD = 3600000;

    private final Vertx vertx;
    private final Set<InetAddress> configuredServers;

    private Set<InetAddress> liveServers;

    private NetAcuityServerAddressProvider(Vertx vertx, Set<InetAddress> configuredServers) {
        this.vertx = vertx;
        this.configuredServers = configuredServers;
        this.liveServers = new CopyOnWriteArraySet<>(configuredServers);
    }

    public static NetAcuityServerAddressProvider create(Vertx vertx, Set<String> serverNames) {
        return new NetAcuityServerAddressProvider(Objects.requireNonNull(vertx), serverAddressesFrom(serverNames));
    }

    private static Set<InetAddress> serverAddressesFrom(Set<String> servers) {
        final Set<InetAddress> configuredServers = new HashSet<>();
        for (String host : servers) {
            final InetAddress serverAddress;
            try {
                serverAddress = InetAddress.getByName(host);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException(String.format("Invalid NetAcuity server address: %s", host), e);
            }
            configuredServers.add(serverAddress);
        }

        if (configuredServers.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("No NetAcuity server addresses were specified: %s", servers));
        }
        return configuredServers;
    }

    void removeServerAddress(InetAddress server) {
        liveServers.remove(server);
    }

    void addServerAddress(InetAddress server) {
        liveServers.add(server);
    }

    public void initialize() {
        vertx.setPeriodic(REFRESH_PERIOD, aLong -> refresh());
    }

    void refresh() {
        if (liveServers.size() != configuredServers.size()) {
            liveServers.addAll(configuredServers);
        }
    }

    public InetAddress getServerAddress() {
        return liveServers.stream()
                .min(Comparator.comparing(ignored -> RANDOM.nextBoolean() ? 1 : -1))
                .orElseThrow(() -> new PreBidException("No NetAcuity server address available"));
    }
}
