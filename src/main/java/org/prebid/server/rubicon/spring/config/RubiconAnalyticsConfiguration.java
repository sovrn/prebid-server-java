package org.prebid.server.rubicon.spring.config;

import io.vertx.core.http.HttpClient;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.prebid.server.rubicon.analytics.RubiconAnalyticsModule;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Clock;

@Configuration
@ConditionalOnProperty(prefix = "analytics.rp", name = "host-url")
public class RubiconAnalyticsConfiguration {

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    @Primary
    RubiconAnalyticsModule rubiconAnalyticsModule(RubiconAnalyticsModuleProperties properties,
                                                  HttpClient httpClient, Clock clock) {
        return new RubiconAnalyticsModule(properties.getHostUrl(), properties.getSamplingFactor(),
                properties.getPbsVersion(), httpClient, clock);
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
