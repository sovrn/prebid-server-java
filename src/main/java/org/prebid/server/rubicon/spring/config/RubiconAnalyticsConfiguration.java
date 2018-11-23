package org.prebid.server.rubicon.spring.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.prebid.server.auction.ImplicitParametersExtractor;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.cookie.UidsCookieService;
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

import javax.validation.constraints.NotBlank;
import java.util.Collections;
import java.util.Map;

@Configuration
@ConditionalOnProperty(prefix = "analytics.rp", name = "enabled", havingValue = "true")
public class RubiconAnalyticsConfiguration {

    @Bean
    @Primary
    RubiconAnalyticsModule rubiconAnalyticsModule(
            @Value("${external-url}") String externalUrl,
            @Value("${datacenter-region}") String dataCenterRegion,
            RubiconAnalyticsModuleProperties properties,
            ImplicitParametersExtractor implicitParametersExtractor,
            BidderCatalog bidderCatalog,
            UidsCookieService uidsCookieService,
            HttpClient httpClient) {

        final SamplingFactor samplingFactor = properties.getSamplingFactor();

        return new RubiconAnalyticsModule(properties.getHostUrl(), samplingFactor.getGlobal(),
                ObjectUtils.defaultIfNull(samplingFactor.getAccount(), Collections.emptyMap()),
                properties.getPbsVersion(), implicitParametersExtractor.domainFrom(externalUrl), dataCenterRegion,
                bidderCatalog, uidsCookieService, httpClient);
    }

    @Component
    @ConfigurationProperties(prefix = "analytics.rp")
    @ConditionalOnProperty(prefix = "analytics.rp", name = "enabled", havingValue = "true")
    @Validated
    @Data
    @NoArgsConstructor
    private static class RubiconAnalyticsModuleProperties {

        @NotBlank
        private String hostUrl;

        private String pbsVersion;

        private SamplingFactor samplingFactor = new SamplingFactor();
    }

    @Data
    @NoArgsConstructor
    private static class SamplingFactor {

        private Integer global;

        private Map<Integer, Integer> account;
    }
}
