package org.prebid.server.spring.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.deals.DeliveryProgressService;
import org.prebid.server.deals.simulation.DealsSimulationAdminHandler;
import org.prebid.server.handler.AccountCacheInvalidationHandler;
import org.prebid.server.handler.AdminHandler;
import org.prebid.server.handler.CurrencyRatesHandler;
import org.prebid.server.handler.CustomizedAdminEndpoint;
import org.prebid.server.handler.DealsStatusHandler;
import org.prebid.server.handler.LineItemStatusHandler;
import org.prebid.server.handler.SettingsCacheNotificationHandler;
import org.prebid.server.handler.TracerLogHandler;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.log.CriteriaManager;
import org.prebid.server.manager.AdminManager;
import org.prebid.server.settings.CachingApplicationSettings;
import org.prebid.server.settings.SettingsCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;

@Configuration
public class AdminEndpointsConfiguration {
    private static final String DEALS_SIMULATION_ENDPOINT = "/pbs-admin/e2eAdmin/*";
    private static final String DEALS_STATUS_ENDPOINT = "/pbs-admin/deals-status";
    private static final String DEALS_LINEITEM_STATUS_ENDPOINT = "/pbs-admin/lineitem-status";
    private static final String CURRENCY_RATES_ENDPOINT = "/pbs-admin/currency-rates";
    private static final String STOREDREQUESTS_OPENRTB_ENDPOINT = "/pbs-admin/storedrequests/openrtb2";
    private static final String STOREDREQUESTS_AMP_ENDPOINT = "/pbs-admin/storedrequests/amp";
    private static final String CACHE_INVALIDATE_ENDPOINT = "/pbs-admin/cache/invalidate";
    private static final String TRACELOG_ENDPOINT = "/pbs-admin/tracelog";
    private static final String ADMIN_ENDPOINT = "/pbs-admin/admin";

    @Bean
    @ConditionalOnExpression("${deals.enabled} == true and ${admin-endpoints.deals-status.enabled} == true")
    CustomizedAdminEndpoint dealsStatusEndpoint(
            DeliveryProgressService deliveryProgressService,
            JacksonMapper mapper,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.deals-status.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.deals-status.protected}") boolean isProtected) {

        final DealsStatusHandler dealsStatusHandler = new DealsStatusHandler(deliveryProgressService, mapper);
        return new CustomizedAdminEndpoint(DEALS_STATUS_ENDPOINT, dealsStatusHandler, isOnApplicationPort, isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    @ConditionalOnExpression("${deals.enabled} == true and ${admin-endpoints.lineitem-status.enabled} == true")
    CustomizedAdminEndpoint lineItemStatusEndpoint(
            DeliveryProgressService deliveryProgressService,
            JacksonMapper mapper,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.lineitem-status.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.lineitem-status.protected}") boolean isProtected) {

        final LineItemStatusHandler lineItemStatusHandler = new LineItemStatusHandler(deliveryProgressService, mapper);
        return new CustomizedAdminEndpoint(DEALS_LINEITEM_STATUS_ENDPOINT, lineItemStatusHandler, isOnApplicationPort,
                isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    @ConditionalOnExpression("${deals.enabled} == true and ${deals.simulation.enabled} == true"
            + " and ${admin-endpoints.e2eadmin.enabled} == true")
    CustomizedAdminEndpoint dealsSimulationAdminEndpoint(
            DealsSimulationAdminHandler dealsSimulationAdminHandler,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.e2eadmin.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.e2eadmin.protected}") boolean isProtected) {

        return new CustomizedAdminEndpoint(DEALS_SIMULATION_ENDPOINT, dealsSimulationAdminHandler, isOnApplicationPort,
                isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    @ConditionalOnExpression("${currency-converter.external-rates.enabled} == true"
            + " and ${admin-endpoints.currency-rates.enabled} == true")
    CustomizedAdminEndpoint currencyConversionRatesEndpoint(
            CurrencyConversionService currencyConversionRates,
            JacksonMapper mapper,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.currency-rates.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.currency-rates.protected}") boolean isProtected) {

        final CurrencyRatesHandler currencyRatesHandler = new CurrencyRatesHandler(currencyConversionRates, mapper);
        return new CustomizedAdminEndpoint(CURRENCY_RATES_ENDPOINT, currencyRatesHandler, isOnApplicationPort,
                isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    @ConditionalOnExpression("${settings.in-memory-cache.notification-endpoints-enabled} == true"
            + " and ${admin-endpoints.storedrequest.enabled} == true")
    CustomizedAdminEndpoint cacheNotificationEndpoint(
            SettingsCache settingsCache,
            JacksonMapper mapper,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.storedrequest.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.storedrequest.protected}") boolean isProtected) {

        final SettingsCacheNotificationHandler cacheNotificationHandler =
                new SettingsCacheNotificationHandler(settingsCache, mapper);

        return new CustomizedAdminEndpoint(STOREDREQUESTS_OPENRTB_ENDPOINT, cacheNotificationHandler,
                isOnApplicationPort, isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    @ConditionalOnExpression("${settings.in-memory-cache.notification-endpoints-enabled} == true"
            + " and ${admin-endpoints.storedrequest-amp.enabled} == true")
    CustomizedAdminEndpoint ampCacheNotificationEndpoint(
            SettingsCache ampSettingsCache,
            JacksonMapper mapper,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.storedrequest-amp.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.storedrequest-amp.protected}") boolean isProtected) {

        final SettingsCacheNotificationHandler settingsCacheNotificationHandler =
                new SettingsCacheNotificationHandler(ampSettingsCache, mapper);

        return new CustomizedAdminEndpoint(STOREDREQUESTS_AMP_ENDPOINT, settingsCacheNotificationHandler,
                isOnApplicationPort, isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    @ConditionalOnExpression("${settings.in-memory-cache.notification-endpoints-enabled} == true"
            + " and ${admin-endpoints.cache-invalidation.enabled} == true")
    CustomizedAdminEndpoint cacheInvalidateNotificationEndpoint(
            CachingApplicationSettings cachingApplicationSettings,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.cache-invalidation.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.cache-invalidation.protected}") boolean isProtected) {

        final AccountCacheInvalidationHandler accountCacheInvalidationHandler =
                new AccountCacheInvalidationHandler(cachingApplicationSettings);

        return new CustomizedAdminEndpoint(CACHE_INVALIDATE_ENDPOINT, accountCacheInvalidationHandler,
                isOnApplicationPort, isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    @ConditionalOnProperty(prefix = "admin-endpoints.tracelog", name = "enabled", havingValue = "true")
    CustomizedAdminEndpoint tracerLogEndpoint(
            CriteriaManager criteriaManager,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.tracelog.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.tracelog.protected}") boolean isProtected) {

        final TracerLogHandler tracerLogHandler = new TracerLogHandler(criteriaManager);
        return new CustomizedAdminEndpoint(TRACELOG_ENDPOINT, tracerLogHandler, isOnApplicationPort, isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    @ConditionalOnProperty(prefix = "admin-endpoints.logger-level-modifier", name = "enabled", havingValue = "true")
    CustomizedAdminEndpoint adminHandler(
            AdminManager adminManager,
            @Autowired(required = false) Map<String, String> adminEndpointCredentials,
            @Value("${admin-endpoints.logger-level-modifier.on-application-port}") boolean isOnApplicationPort,
            @Value("${admin-endpoints.logger-level-modifier.protected}") boolean isProtected) {
        final AdminHandler adminHandler = new AdminHandler(adminManager);
        return new CustomizedAdminEndpoint(ADMIN_ENDPOINT, adminHandler, isOnApplicationPort, isProtected)
                .credentials(adminEndpointCredentials);
    }

    @Bean
    Map<String, String> adminEndpointCredentials(
            @Autowired(required = false) AdminEndpointCredentials adminEndpointCredentials) {

        return adminEndpointCredentials == null
                ? Collections.emptyMap()
                : adminEndpointCredentials.getCredentials();
    }

    @Component
    @ConfigurationProperties(prefix = "admin-endpoints")
    @Data
    @NoArgsConstructor
    public static class AdminEndpointCredentials {

        private Map<String, String> credentials;
    }
}
