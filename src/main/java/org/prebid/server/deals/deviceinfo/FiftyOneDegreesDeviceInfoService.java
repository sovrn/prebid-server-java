package org.prebid.server.deals.deviceinfo;

import fiftyone.mobile.detection.Match;
import fiftyone.mobile.detection.Provider;
import fiftyone.mobile.detection.entities.Values;
import fiftyone.mobile.detection.factories.StreamFactory;
import io.vertx.core.Future;
import org.prebid.server.deals.model.DeviceInfo;
import org.prebid.server.deals.model.DeviceType;
import org.prebid.server.exception.PreBidException;

import java.io.IOException;

/**
 * Implementation of {@link DeviceInfoService} by <a href="https://51degrees.com/device-detection">51degrees</a>
 */
public class FiftyOneDegreesDeviceInfoService implements DeviceInfoService {

    private static final String VENDOR = "fiftyonedegrees";

    private Provider provider;

    /**
     * Initializes {@link Provider} from specified file.
     */
    public void setDataPath(String dataPath) {
        try {
            provider = new Provider(StreamFactory.create(dataPath));
        } catch (IOException e) {
            throw new PreBidException(String.format("Cant create Provider for dataPath: %s", dataPath));
        }
    }

    @Override
    public Future<DeviceInfo> getDeviceInfo(String ua) {
        if (provider == null) {
            return Future.failedFuture("51Degrees data file hasn't been downloaded yet, try again later");
        }

        try {
            final Match match = provider.match(ua);
            return Future.succeededFuture(createDeviceInfo(match));
        } catch (IOException e) {
            return Future.failedFuture(new PreBidException("Problem accessing deviceInfo data file"));
        }
    }

    private static DeviceInfo createDeviceInfo(Match match) throws IOException {
        final String platformName = getValueByPropertyName(match, "PlatformName");
        final String deviceTypeRaw = getValueByPropertyName(match, "DeviceType");

        return DeviceInfo.builder()
                .vendor(VENDOR)
                .deviceType(DeviceType.resolveDeviceType(deviceTypeRaw))
                .deviceTypeRaw(deviceTypeRaw)
                .osfamily(platformName)
                .os(platformName)
                .osVersion(getValueByPropertyName(match, "PlatformVersion"))
                // might be different than the one by DeviceAtlas (https://51degrees.com/resources/property-dictionary)
                .manufacturer(getValueByPropertyName(match, "HardwareVendor"))
                .model(getValueByPropertyName(match, "HardwareName"))
                .browser(getValueByPropertyName(match, "BrowserName"))
                .browserVersion(getValueByPropertyName(match, "BrowserVersion"))
                // might be different than the one by DeviceAtlas (https://51degrees.com/resources/property-dictionary)
                .carrier(getValueByPropertyName(match, "HardwareCarrier"))
                .build();
    }

    private static String getValueByPropertyName(Match match, String propertyName) throws IOException {
        final Values values = match.getValues(propertyName);
        return values == null ? null : values.toString();
    }
}
