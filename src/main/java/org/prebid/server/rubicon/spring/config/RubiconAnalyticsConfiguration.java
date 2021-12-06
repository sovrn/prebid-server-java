package org.prebid.server.rubicon.spring.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.prebid.server.auction.IpAddressHelper;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.geolocation.CountryCodeMapper;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.rubicon.analytics.RubiconAnalyticsReporter;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.util.VersionInfo;
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
    RubiconAnalyticsReporter rubiconAnalyticsReporter(@Value("${external-url}") String externalUrl,
                                                      @Value("${datacenter-region}") String dataCenterRegion,
                                                      RubiconAnalyticsReporterProperties properties,
                                                      VersionInfo versionInfo,
                                                      BidderCatalog bidderCatalog,
                                                      UidsCookieService uidsCookieService,
                                                      UidsAuditCookieService uidsAuditCookieService,
                                                      CurrencyConversionService currencyConversionService,
                                                      CountryCodeMapper countryCodeMapper,
                                                      IpAddressHelper ipAddressHelper,
                                                      HttpClient httpClient,
                                                      JacksonMapper mapper) {

        final AnalyticsLogs analyticsLogs = properties.getLog();
        final boolean logEmptyDimensions = analyticsLogs != null && analyticsLogs.emptyDimensions;

        return new RubiconAnalyticsReporter(
                properties.getHostUrl(),
                properties.getSamplingFactor(),
                versionInfo.getVersion(),
                HttpUtil.getHostFromUrl(externalUrl),
                properties.getHostVendorId(),
                dataCenterRegion,
                bidderCatalog,
                uidsCookieService,
                uidsAuditCookieService,
                currencyConversionService,
                countryCodeMapper,
                ipAddressHelper,
                httpClient,
                logEmptyDimensions,
                mapper);
    }

    @lombok.Value(staticConstructor = "of")
    private static class GitRevision {

        @JsonProperty("git.build.version")
        String buildVersion;

        static GitRevision empty() {
            return of(null);
        }
    }

    @Component
    @ConfigurationProperties(prefix = "analytics.rp")
    @ConditionalOnProperty(prefix = "analytics.rp", name = "enabled", havingValue = "true")
    @Validated
    @Data
    @NoArgsConstructor
    private static class RubiconAnalyticsReporterProperties {

        @NotBlank
        private String hostUrl;

        private Integer hostVendorId;

        private Integer samplingFactor;

        private AnalyticsLogs log;
    }

    @Data
    private static class AnalyticsLogs {

        private boolean emptyDimensions;
    }
}
