package org.prebid.server.rubicon.spring.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.cookie.UidsCookieService;
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.rubicon.analytics.RubiconAnalyticsModule;
import org.prebid.server.rubicon.audit.UidsAuditCookieService;
import org.prebid.server.util.HttpUtil;
import org.prebid.server.util.ResourceUtil;
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
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Configuration
@ConditionalOnProperty(prefix = "analytics.rp", name = "enabled", havingValue = "true")
public class RubiconAnalyticsConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(RubiconAnalyticsConfiguration.class);

    private static final String VERSION_UNDEFINED = "undefined";

    @Bean
    @Primary
    RubiconAnalyticsModule rubiconAnalyticsModule(@Value("${external-url}") String externalUrl,
                                                  @Value("${datacenter-region}") String dataCenterRegion,
                                                  RubiconAnalyticsModuleProperties properties,
                                                  BidderCatalog bidderCatalog,
                                                  UidsCookieService uidsCookieService,
                                                  UidsAuditCookieService uidsAuditCookieService,
                                                  CurrencyConversionService currencyConversionService,
                                                  HttpClient httpClient,
                                                  JacksonMapper mapper) {

        return new RubiconAnalyticsModule(
                properties.getHostUrl(),
                properties.getSamplingFactor(),
                pbsVersion("git-revision.json", mapper),
                HttpUtil.getDomainFromUrl(externalUrl),
                dataCenterRegion,
                bidderCatalog,
                uidsCookieService,
                uidsAuditCookieService,
                currencyConversionService,
                httpClient,
                mapper);
    }

    private static String pbsVersion(String gitRevisionPath, JacksonMapper mapper) {
        final GitRevision gitRevision = readGitRevision(mapper, gitRevisionPath);
        final String buildVersion = gitRevision.getBuildVersion();
        final String extractedVersion = buildVersion != null ? extractVersion(buildVersion) : null;

        return extractedVersion != null ? extractedVersion : VERSION_UNDEFINED;
    }

    private static GitRevision readGitRevision(JacksonMapper mapper, String gitRevisionPath) {
        try {
            return mapper.mapper().readValue(ResourceUtil.readFromClasspath(gitRevisionPath), GitRevision.class);
        } catch (IllegalArgumentException | IOException e) {
            logger.warn("Was not able to read revision file {0}. Reason: {1}", gitRevisionPath, e.getMessage());
            return GitRevision.empty();
        }
    }

    private static String extractVersion(String buildVersion) {
        final Pattern versionPattern = Pattern.compile("\\d+\\.\\d+\\.\\d");
        final Matcher versionMatcher = versionPattern.matcher(buildVersion);

        return versionMatcher.lookingAt() ? versionMatcher.group() : null;
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
    private static class RubiconAnalyticsModuleProperties {

        @NotBlank
        private String hostUrl;

        private Integer samplingFactor;
    }
}
