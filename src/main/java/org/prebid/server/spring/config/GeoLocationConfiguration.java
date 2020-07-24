package org.prebid.server.spring.config;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import org.prebid.server.execution.RemoteFileSyncer;
import org.prebid.server.geolocation.CircuitBreakerSecuredGeoLocationService;
import org.prebid.server.geolocation.GeoLocationService;
import org.prebid.server.geolocation.MaxMindGeoLocationService;
import org.prebid.server.metric.Metrics;
import org.prebid.server.rubicon.geolocation.CircuitBreakerSecuredNetAcuityGeoLocationService;
import org.prebid.server.rubicon.geolocation.NetAcuityGeoLocationService;
import org.prebid.server.rubicon.geolocation.NetAcuityServerAddressProvider;
import org.prebid.server.spring.config.model.CircuitBreakerProperties;
import org.prebid.server.spring.config.model.HttpClientProperties;
import org.prebid.server.spring.config.model.RemoteFileSyncerProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@ConditionalOnProperty(prefix = "geolocation", name = "enabled", havingValue = "true")
public class GeoLocationConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "geolocation.circuit-breaker")
    @ConditionalOnProperty(prefix = "geolocation.circuit-breaker", name = "enabled", havingValue = "true")
    CircuitBreakerProperties geolocationCircuitBreakerProperties() {
        return new CircuitBreakerProperties();
    }

    @Configuration
    @ConditionalOnProperty(prefix = "geolocation", name = "type", havingValue = "maxmind")
    static class MaxMindGeoLocationConfiguration {

        @Bean
        @ConfigurationProperties(prefix = "geolocation.maxmind.remote-file-syncer")
        RemoteFileSyncerProperties maxMindRemoteFileSyncerProperties() {
            return new RemoteFileSyncerProperties();
        }

        /**
         * Default geolocation service implementation.
         */
        @Bean
        @ConditionalOnProperty(prefix = "geolocation.circuit-breaker", name = "enabled", havingValue = "false",
                matchIfMissing = true)
        GeoLocationService basicGeoLocationService(RemoteFileSyncerProperties maxMindRemoteFileSyncerProperties,
                                                   Vertx vertx) {

            return createGeoLocationService(maxMindRemoteFileSyncerProperties, vertx);
        }

        @Bean
        @ConditionalOnProperty(prefix = "geolocation.circuit-breaker", name = "enabled", havingValue = "true")
        CircuitBreakerSecuredGeoLocationService circuitBreakerSecuredGeoLocationService(
                Vertx vertx,
                Metrics metrics,
                RemoteFileSyncerProperties maxMindRemoteFileSyncerProperties,
                @Qualifier("geolocationCircuitBreakerProperties") CircuitBreakerProperties circuitBreakerProperties,
                Clock clock) {

            return new CircuitBreakerSecuredGeoLocationService(vertx,
                    createGeoLocationService(maxMindRemoteFileSyncerProperties, vertx), metrics,
                    circuitBreakerProperties.getOpeningThreshold(), circuitBreakerProperties.getOpeningIntervalMs(),
                    circuitBreakerProperties.getClosingIntervalMs(), clock);
        }

        private GeoLocationService createGeoLocationService(
                RemoteFileSyncerProperties maxMindRemoteFileSyncerProperties, Vertx vertx) {

            final HttpClientProperties httpClientProperties = maxMindRemoteFileSyncerProperties.getHttpClient();
            final HttpClientOptions httpClientOptions = new HttpClientOptions()
                    .setConnectTimeout(httpClientProperties.getConnectTimeoutMs())
                    .setMaxRedirects(httpClientProperties.getMaxRedirects());

            final RemoteFileSyncer remoteFileSyncer =
                    RemoteFileSyncer.create(
                            maxMindRemoteFileSyncerProperties.getDownloadUrl(),
                            maxMindRemoteFileSyncerProperties.getSaveFilepath(),
                            maxMindRemoteFileSyncerProperties.getTmpFilepath(),
                            maxMindRemoteFileSyncerProperties.getRetryCount(),
                            maxMindRemoteFileSyncerProperties.getRetryIntervalMs(),
                            maxMindRemoteFileSyncerProperties.getTimeoutMs(),
                            maxMindRemoteFileSyncerProperties.getUpdateIntervalMs(),
                            vertx.createHttpClient(httpClientOptions), vertx, vertx.fileSystem());
            final MaxMindGeoLocationService maxMindGeoLocationService = new MaxMindGeoLocationService();

            remoteFileSyncer.syncForFilepath(maxMindGeoLocationService);
            return maxMindGeoLocationService;
        }
    }

    @Configuration
    @ConditionalOnProperty(prefix = "geolocation", name = "type", havingValue = "netacuity")
    static class NetAcuityGeoLocationConfiguration {

        @Bean
        NetAcuityServerAddressProvider netAcuityAddressProvider(
                Vertx vertx, @Value("${geolocation.netacuity.server}") String server) {
            return NetAcuityServerAddressProvider.create(vertx, parseServerNames(server));
        }

        @Bean
        @ConditionalOnProperty(prefix = "geolocation.circuit-breaker", name = "enabled", havingValue = "false",
                matchIfMissing = true)
        NetAcuityGeoLocationService netAcuityGeoLocationService(
                Vertx vertx,
                NetAcuityServerAddressProvider addressProvider,
                Clock clock,
                Metrics metrics) {
            return createNetAcuityGeoLocationService(vertx, addressProvider, clock, metrics);
        }

        @Bean
        @ConditionalOnProperty(prefix = "geolocation.circuit-breaker", name = "enabled", havingValue = "true")
        CircuitBreakerSecuredNetAcuityGeoLocationService circuitBreakerSecuredNetAcuityGeoLocationService(
                Vertx vertx,
                NetAcuityServerAddressProvider netAcuityServerAddressProvider,
                @Qualifier("geolocationCircuitBreakerProperties") CircuitBreakerProperties circuitBreakerProperties,
                Clock clock,
                Metrics metrics) {

            return new CircuitBreakerSecuredNetAcuityGeoLocationService(
                    createNetAcuityGeoLocationService(vertx, netAcuityServerAddressProvider, clock, metrics),
                    netAcuityServerAddressProvider, vertx, circuitBreakerProperties.getOpeningThreshold(),
                    circuitBreakerProperties.getOpeningIntervalMs(), circuitBreakerProperties.getClosingIntervalMs(),
                    clock);
        }

        private static NetAcuityGeoLocationService createNetAcuityGeoLocationService(
                Vertx vertx, NetAcuityServerAddressProvider addressProvider, Clock clock, Metrics metrics) {
            return new NetAcuityGeoLocationService(vertx, addressProvider::getServerAddress, clock, metrics);
        }

        private static Set<String> parseServerNames(String serversString) {
            Objects.requireNonNull(serversString);
            return Stream.of(serversString.split(",")).map(String::trim).collect(Collectors.toSet());
        }
    }
}
