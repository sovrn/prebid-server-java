package org.prebid.server.rubicon.spring.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.rubicon.analytics.RubiconAnalyticsModule;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.util.HttpUtil;
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

@Configuration
@ConditionalOnProperty(prefix = "analytics.rp", name = "enabled", havingValue = "true")
public class RubiconAnalyticsConfiguration {

    @Bean
    @Primary
    RubiconAnalyticsModule rubiconAnalyticsModule(
            @Value("${external-url}") String externalUrl,
            @Value("${datacenter-region}") String dataCenterRegion,
            RubiconAnalyticsModuleProperties properties,
            BidderCatalog bidderCatalog,
            UidsCookieService uidsCookieService,
            UidsAuditCookieService uidsAuditCookieService,
            HttpClient httpClient,
            JacksonMapper mapper) {

        return new RubiconAnalyticsModule(properties.getHostUrl(), properties.getSamplingFactor(),
                properties.getPbsVersion(), HttpUtil.getDomainFromUrl(externalUrl), dataCenterRegion,
                bidderCatalog, uidsCookieService, uidsAuditCookieService, httpClient, mapper);
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

        private Integer samplingFactor;
    }
}
