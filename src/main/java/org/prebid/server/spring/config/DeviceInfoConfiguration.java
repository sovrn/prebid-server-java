package org.prebid.server.spring.config;

import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import org.prebid.server.deals.deviceinfo.DeviceAtlasDeviceInfoService;
import org.prebid.server.execution.RemoteFileSyncer;
import org.prebid.server.spring.config.model.HttpClientProperties;
import org.prebid.server.spring.config.model.RemoteFileSyncerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(prefix = "device-info", name = "enabled", havingValue = "true")
public class DeviceInfoConfiguration {

    @Bean
    DeviceAtlasDeviceInfoService deviceAtlasDeviceInfoService(
            RemoteFileSyncerProperties deviceAtlasRemoteFileSyncerProperties,
            Vertx vertx,
            FileSystem fileSystem) {

        return createDeviceInfoService(deviceAtlasRemoteFileSyncerProperties, vertx, fileSystem);
    }

    @Bean
    @ConfigurationProperties(prefix = "device-info.device-atlas.remote-file-syncer")
    RemoteFileSyncerProperties deviceAtlasRemoteFileSyncerProperties() {
        return new RemoteFileSyncerProperties();
    }

    private DeviceAtlasDeviceInfoService createDeviceInfoService(
            RemoteFileSyncerProperties fileSyncerProperties, Vertx vertx, FileSystem fileSystem) {

        final HttpClientProperties httpClientProperties = fileSyncerProperties.getHttpClient();
        final HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setConnectTimeout(httpClientProperties.getConnectTimeoutMs())
                .setMaxRedirects(httpClientProperties.getMaxRedirects());
        final HttpClient httpClient = vertx.createHttpClient(httpClientOptions);

        final RemoteFileSyncer remoteFileSyncer = RemoteFileSyncer.create(fileSyncerProperties.getDownloadUrl(),
                fileSyncerProperties.getSaveFilepath(), fileSyncerProperties.getTmpFilepath(),
                fileSyncerProperties.getRetryCount(), fileSyncerProperties.getRetryIntervalMs(),
                fileSyncerProperties.getTimeoutMs(), fileSyncerProperties.getUpdateIntervalMs(),
                httpClient, vertx, fileSystem);

        final DeviceAtlasDeviceInfoService deviceInfoService = new DeviceAtlasDeviceInfoService();
        remoteFileSyncer.syncForFilepath(deviceInfoService);
        return deviceInfoService;
    }
}
