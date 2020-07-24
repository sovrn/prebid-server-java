package org.prebid.server.deals.deviceinfo;

import io.vertx.core.Future;
import mobi.mtld.da.Properties;
import mobi.mtld.da.Property;
import mobi.mtld.da.device.DeviceApi;
import mobi.mtld.da.exception.DataReadException;
import mobi.mtld.da.exception.JsonException;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.deals.model.DeviceInfo;
import org.prebid.server.deals.model.DeviceType;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.RemoteFileProcessor;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Implementation of {@link DeviceInfoService}
 * based on <a href="https://deviceatlas.com/device-data/properties">DeviceAtlas</a>
 */
public class DeviceAtlasDeviceInfoService implements DeviceInfoService, RemoteFileProcessor {

    private static final String VENDOR = "deviceatlas";

    private static final String DATA_FILE_EXTENSION = ".json.gz";

    private DeviceApi deviceApi;

    /**
     * Initializes {@link DeviceApi} from specified file.
     */
    public Future<?> setDataPath(String dataFilePath) {
        if (!dataFilePath.endsWith(DATA_FILE_EXTENSION)) {
            throw new PreBidException(String.format("DeviceAtlas data file %s has wrong file extension, expected: %s",
                    dataFilePath, DATA_FILE_EXTENSION));
        }

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(dataFilePath))) {
            final DeviceApi deviceApi = new DeviceApi();
            deviceApi.loadDataFromStream(gzipInputStream);
            this.deviceApi = deviceApi;

            return Future.succeededFuture();
        } catch (JsonException | IOException | DataReadException e) {
            return Future.failedFuture(String.format(
                    "Loading DeviceAtlas data file from path %s occurs error: %s", dataFilePath, e.getMessage()));
        }
    }

    @Override
    public Future<DeviceInfo> getDeviceInfo(String ua) {
        if (deviceApi == null) {
            return Future.failedFuture("DeviceAtlas data file hasn't been downloaded yet, try again later");
        }

        if (StringUtils.isBlank(ua)) {
            return Future.failedFuture("Failed to get device info in a reason: User Agent was not defined in request.");
        }

        final Properties properties = deviceApi.getProperties(ua);
        if (properties == null) {
            return Future.failedFuture(String.format("Cannot read properties from user-agent string: %s", ua));
        }

        return Future.succeededFuture(createDeviceInfo(properties));
    }

    private static DeviceInfo createDeviceInfo(Properties properties) {
        final String primaryHardwareType = getProperty(properties, "primaryHardwareType");

        return DeviceInfo.builder()
                .vendor(VENDOR)
                .deviceType(DeviceType.resolveDeviceType(primaryHardwareType))
                .deviceTypeRaw(primaryHardwareType)
                .osfamily(getProperty(properties, "osFamily"))
                .os(getProperty(properties, "osName"))
                .osVersion(getProperty(properties, "osVersion"))
                .browser(getProperty(properties, "browserName"))
                .browserVersion(getProperty(properties, "browserVersion"))
                // vendor property might be too wide (https://deviceatlas.com/resources/available-properties), mb it's
                // better to use "manufacturer" instead (given that we have a "carrier")?
                .manufacturer(getProperty(properties, "vendor"))
                .model(getProperty(properties, "marketingName"))
                .language(getProperty(properties, "language"))
                .carrier(getProperty(properties, "networkBrand"))
                .build();
    }

    private static String getProperty(Properties properties, String name) {
        final Property property = properties.get(name);
        return property == null ? null : StringUtils.trimToNull(property.asString());
    }
}
