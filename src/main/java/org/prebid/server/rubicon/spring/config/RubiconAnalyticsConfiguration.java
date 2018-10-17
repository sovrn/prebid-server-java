package org.prebid.server.rubicon.spring.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.prebid.server.auction.ImplicitParametersExtractor;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.rubicon.analytics.RubiconAnalyticsModule;
import org.prebid.server.vertx.http.HttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Configuration
@ConditionalOnProperty(prefix = "analytics.rp", name = "host-url")
public class RubiconAnalyticsConfiguration {

    @Bean
    @Primary
    RubiconAnalyticsModule rubiconAnalyticsModule(
            @Value("${external-url}") String externalUrl,
            @Value("${datacenter-region}") String dataCenterRegion,
            RubiconAnalyticsModuleProperties properties,
            ImplicitParametersExtractor implicitParametersExtractor,
            BidderCatalog bidderCatalog,
            HttpClient httpClient) {

        return new RubiconAnalyticsModule(properties.getHostUrl(), properties.getSamplingFactor(),
                properties.getPbsVersion(), implicitParametersExtractor.domainFrom(externalUrl), dataCenterRegion,
                bidderCatalog, httpClient);
    }

    @Component
    @ConfigurationProperties(prefix = "analytics.rp")
    @ConditionalOnProperty(prefix = "analytics.rp", name = "host-url")
    @Validated
    @Data
    @NoArgsConstructor
    private static class RubiconAnalyticsModuleProperties {

        @NotBlank
        private String hostUrl;
        @NotNull
        @Min(1)
        private Integer samplingFactor;
        private String pbsVersion;
    }
}
