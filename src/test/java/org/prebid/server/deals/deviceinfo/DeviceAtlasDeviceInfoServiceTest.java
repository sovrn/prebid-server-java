package org.prebid.server.deals.deviceinfo;

import io.vertx.core.Future;
import mobi.mtld.da.Properties;
import mobi.mtld.da.Property;
import mobi.mtld.da.device.DeviceApi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.deals.model.DeviceInfo;
import org.prebid.server.deals.model.DeviceType;
import org.prebid.server.exception.PreBidException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

public class DeviceAtlasDeviceInfoServiceTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private DeviceApi deviceApi;

    private DeviceAtlasDeviceInfoService deviceInfoService;
    @Mock
    private Properties properties;

    @Before
    public void setUp() {
        given(deviceApi.getProperties(anyString())).willReturn(properties);
        deviceInfoService = new DeviceAtlasDeviceInfoService();
    }

    @Test
    public void setDatabaseReaderShouldThrowExceptionWhenWrongFileExtension() {
        assertThatExceptionOfType(PreBidException.class)
                .isThrownBy(() -> deviceInfoService.setDataPath("no_file"))
                .withMessage("DeviceAtlas data file no_file has wrong file extension, expected: .json.gz");
    }

    @Test
    public void setDatabaseReaderShouldReturnFailedFutureWhenDatabaseArchiveNotFound() {
        // given and when
        final Future<?> result = deviceInfoService.setDataPath("no_file.json.gz");

        // then
        assertTrue(result.failed());
        assertThat(result.cause()).hasMessageStartingWith(
                "Loading DeviceAtlas data file from path no_file.json.gz occurs error: no_file.json.gz");
    }

    @Test
    public void getDeviceInfoShouldReturnFailedFutureWhenDeviceAtlasFileNotDownloaded() {
        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo("smth");

        // then
        assertThat(resultFuture.failed()).isTrue();
        assertThat(resultFuture.cause())
                .hasMessage("DeviceAtlas data file hasn't been downloaded yet, try again later");
    }

    @Test
    public void getDeviceInfoShouldReturnFailedFutureWhenUserAgentWasNotDefined() throws NoSuchFieldException {
        // when
        givenDownloadedFile();
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(null);

        // then
        assertThat(resultFuture.failed()).isTrue();
        assertThat(resultFuture.cause())
                .hasMessage("Failed to get device info in a reason: User Agent was not defined in request.");
    }

    @Test
    public void getDeviceInfoShouldReturnFailedFutureWhenPropertiesIsNull() throws NoSuchFieldException {
        // given
        givenDownloadedFile();
        given(deviceApi.getProperties(anyString())).willReturn(null);

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo("smth");

        // then
        assertThat(resultFuture.failed()).isTrue();
        assertThat(resultFuture.cause()).hasMessage("Cannot read properties from user-agent string: smth");
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoWithAndroid() throws NoSuchFieldException {
        // given
        givenDownloadedFile();

        final String ua = "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) "
                + "AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19";
        given(properties.get("primaryHardwareType")).willReturn(givenProperty("Mobile Phone"));
        given(properties.get("osName")).willReturn(givenProperty("Android"));
        given(properties.get("osFamily")).willReturn(givenProperty("Android"));
        given(properties.get("osVersion")).willReturn(givenProperty("4.0.4"));
        given(properties.get("browserName")).willReturn(givenProperty("Chrome Mobile"));
        given(properties.get("browserVersion")).willReturn(givenProperty("18.0.1025.133"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("deviceatlas")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile Phone")
                .osfamily("Android")
                .os("Android")
                .osVersion("4.0.4")
                .browser("Chrome Mobile")
                .browserVersion("18.0.1025.133")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoWithIos() throws NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) "
                + "AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e";
        given(properties.get("primaryHardwareType")).willReturn(givenProperty("Mobile Phone"));
        given(properties.get("osName")).willReturn(givenProperty("iOS"));
        given(properties.get("osFamily")).willReturn(givenProperty("iOS"));
        given(properties.get("osVersion")).willReturn(givenProperty("10_3"));
        given(properties.get("browserName")).willReturn(givenProperty("Chrome Mobile"));
        given(properties.get("browserVersion")).willReturn(givenProperty("56.0.2924.75"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("deviceatlas")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile Phone")
                .osfamily("iOS")
                .os("iOS")
                .osVersion("10_3")
                .browser("Chrome Mobile")
                .browserVersion("56.0.2924.75")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoWithIphone() throws NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) ";
        given(properties.get("primaryHardwareType")).willReturn(givenProperty("Mobile Phone"));
        given(properties.get("osName")).willReturn(givenProperty("iOS"));
        given(properties.get("osFamily")).willReturn(givenProperty("null"));
        given(properties.get("osVersion")).willReturn(givenProperty("10_3"));
        given(properties.get("browserName")).willReturn(givenProperty("Safari"));
        given(properties.get("browserVersion")).willReturn(givenProperty("null"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("deviceatlas")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile Phone")
                .osfamily("null")
                .os("iOS")
                .osVersion("10_3")
                .browser("Safari")
                .browserVersion("null")
                .build();
        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoForTablet() throws NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0";
        given(properties.get("primaryHardwareType")).willReturn(givenProperty("Desktop"));
        given(properties.get("osName")).willReturn(givenProperty("Linux - Ubuntu"));
        given(properties.get("osFamily")).willReturn(givenProperty("Linux"));
        given(properties.get("osVersion")).willReturn(givenProperty("null"));
        given(properties.get("browserName")).willReturn(givenProperty("Firefox"));
        given(properties.get("browserVersion")).willReturn(givenProperty("68.0"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("deviceatlas")
                .deviceType(DeviceType.DESKTOP)
                .deviceTypeRaw("Desktop")
                .osfamily("Linux")
                .os("Linux - Ubuntu")
                .osVersion("null")
                .browser("Firefox")
                .browserVersion("68.0")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoForWindows() throws NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063";
        given(properties.get("primaryHardwareType")).willReturn(givenProperty("Desktop"));
        given(properties.get("osName")).willReturn(givenProperty("Windows 10"));
        given(properties.get("osFamily")).willReturn(givenProperty("Windows"));
        given(properties.get("osVersion")).willReturn(givenProperty("NT 10.0"));
        given(properties.get("browserName")).willReturn(givenProperty("Edge"));
        given(properties.get("browserVersion")).willReturn(givenProperty("15.15063"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("deviceatlas")
                .deviceType(DeviceType.DESKTOP)
                .deviceTypeRaw("Desktop")
                .osfamily("Windows")
                .os("Windows 10")
                .osVersion("NT 10.0")
                .browser("Edge")
                .browserVersion("15.15063")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoForSafari() throws NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) "
                + "AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1";
        given(properties.get("primaryHardwareType")).willReturn(givenProperty("Mobile Phone"));
        given(properties.get("osName")).willReturn(givenProperty("iOS"));
        given(properties.get("osFamily")).willReturn(givenProperty("iOS"));
        given(properties.get("osVersion")).willReturn(givenProperty("10_3_1"));
        given(properties.get("browserName")).willReturn(givenProperty("Safari"));
        given(properties.get("browserVersion")).willReturn(givenProperty("10.0"));
        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("deviceatlas")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile Phone")
                .osfamily("iOS")
                .os("iOS")
                .osVersion("10_3_1")
                .browser("Safari")
                .browserVersion("10.0")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoForInternetExplorerAndWindowsPhone() throws NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)";
        given(properties.get("primaryHardwareType")).willReturn(givenProperty("Mobile Phone"));
        given(properties.get("osName")).willReturn(givenProperty("Windows Phone"));
        given(properties.get("osFamily")).willReturn(givenProperty("Windows Phone"));
        given(properties.get("osVersion")).willReturn(givenProperty("7.5"));
        given(properties.get("browserName")).willReturn(givenProperty("Internet Explorer Mobile"));
        given(properties.get("browserVersion")).willReturn(givenProperty("9.0"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("deviceatlas")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile Phone")
                .osfamily("Windows Phone")
                .os("Windows Phone")
                .osVersion("7.5")
                .browser("Internet Explorer Mobile")
                .browserVersion("9.0")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldTolerateWhenPropertyNotExist() throws NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0";
        given(properties.get("primaryHardwareType")).willReturn(null);
        given(properties.get("osName")).willReturn(null);
        given(properties.get("osFamily")).willReturn(null);
        given(properties.get("osVersion")).willReturn(null);
        given(properties.get("browserName")).willReturn(null);
        given(properties.get("browserVersion")).willReturn(null);

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder().vendor("deviceatlas").build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    private void givenDownloadedFile() throws NoSuchFieldException {
        FieldSetter.setField(deviceInfoService,
                deviceInfoService.getClass().getDeclaredField("deviceApi"), deviceApi);
    }

    private static Property givenProperty(String value) {
        return new Property(value, (byte) 1);
    }
}
