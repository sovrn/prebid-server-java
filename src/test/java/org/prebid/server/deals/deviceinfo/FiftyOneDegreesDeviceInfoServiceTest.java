package org.prebid.server.deals.deviceinfo;

import fiftyone.mobile.detection.Match;
import fiftyone.mobile.detection.Provider;
import fiftyone.mobile.detection.entities.Property;
import fiftyone.mobile.detection.entities.Value;
import fiftyone.mobile.detection.entities.Values;
import io.vertx.core.Future;
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

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;

public class FiftyOneDegreesDeviceInfoServiceTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private Provider provider;

    @Mock
    private Match match;

    @Mock
    private Property property;

    private FiftyOneDegreesDeviceInfoService deviceInfoService;

    @Before
    public void init() throws IOException {
        given(provider.match(anyString())).willReturn(match);

        given(match.getValues(eq("DeviceType"))).willReturn(givenValues("Mobile"));

        deviceInfoService = new FiftyOneDegreesDeviceInfoService();
    }

    @Test
    public void setDataPathShouldThrowPrebidException() {
        // given, when and then
        assertThatThrownBy(() -> deviceInfoService.setDataPath("invalid"))
                .isInstanceOf(PreBidException.class)
                .hasMessage("Cant create Provider for dataPath: invalid");
    }

    @Test
    public void getDeviceInfoShouldReturnFailedFutureWhenFiftyOneDegreesFileNotDownloaded() {
        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo("smth");

        // then
        assertThat(resultFuture.failed()).isTrue();
        assertThat(resultFuture.cause()).hasMessage("51Degrees data file hasn't been downloaded yet, try again later");
    }

    @Test
    public void getDeviceInfoShouldReturnFailedFutureWhenMatchThrowsException()
            throws NoSuchFieldException, IOException {
        // given
        givenDownloadedFile();
        given(provider.match(anyString())).willThrow(IOException.class);

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo("smth");

        // then
        assertThat(resultFuture.failed()).isTrue();
        assertThat(resultFuture.cause()).hasMessage("Problem accessing deviceInfo data file");
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoWithAndroid() throws IOException, NoSuchFieldException {
        // given
        givenDownloadedFile();

        final String ua = "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) "
                + "AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19";
        given(match.getValues(eq("PlatformName"))).willReturn(givenValues("Android"));
        given(match.getValues(eq("PlatformVersion"))).willReturn(givenValues("4.0.4"));
        given(match.getValues(eq("BrowserName"))).willReturn(givenValues("Chrome Mobile"));
        given(match.getValues(eq("BrowserVersion"))).willReturn(givenValues("18"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then

        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("fiftyonedegrees")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile")
                .osfamily("Android")
                .os("Android")
                .osVersion("4.0.4")
                .browser("Chrome Mobile")
                .browserVersion("18")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoWithIos() throws IOException, NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) "
                + "AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e";
        given(match.getValues(eq("PlatformName"))).willReturn(givenValues("iOS"));
        given(match.getValues(eq("PlatformVersion"))).willReturn(givenValues("10.3"));
        given(match.getValues(eq("BrowserName"))).willReturn(givenValues("Chrome for iOS"));
        given(match.getValues(eq("BrowserVersion"))).willReturn(givenValues("56"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("fiftyonedegrees")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile")
                .osfamily("iOS")
                .os("iOS")
                .osVersion("10.3")
                .browser("Chrome for iOS")
                .browserVersion("56")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoWithIphone() throws IOException, NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) ";
        given(match.getValues(eq("PlatformName"))).willReturn(givenValues("iOS"));
        given(match.getValues(eq("PlatformVersion"))).willReturn(givenValues("10.3"));
        given(match.getValues(eq("BrowserName"))).willReturn(givenValues("UCBrowser for iPhone"));
        given(match.getValues(eq("BrowserVersion"))).willReturn(givenValues("11.3"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("fiftyonedegrees")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile")
                .osfamily("iOS")
                .os("iOS")
                .osVersion("10.3")
                .browser("UCBrowser for iPhone")
                .browserVersion("11.3")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoForTablet() throws IOException, NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0";
        given(match.getValues(eq("DeviceType"))).willReturn(givenValues("Tablet"));
        given(match.getValues(eq("PlatformName"))).willReturn(givenValues("Ubuntu"));
        given(match.getValues(eq("PlatformVersion"))).willReturn(givenValues("Unknown"));
        given(match.getValues(eq("BrowserName"))).willReturn(givenValues("Firefox"));
        given(match.getValues(eq("BrowserVersion"))).willReturn(givenValues("57.0"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("fiftyonedegrees")
                .deviceType(DeviceType.TABLET)
                .deviceTypeRaw("Tablet")
                .osfamily("Ubuntu")
                .os("Ubuntu")
                .osVersion("Unknown")
                .browser("Firefox")
                .browserVersion("57.0")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoForWindows() throws IOException, NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 Edge/15.15063";
        given(match.getValues(eq("DeviceType"))).willReturn(givenValues("Desktop"));
        given(match.getValues(eq("PlatformName"))).willReturn(givenValues("Windows"));
        given(match.getValues(eq("PlatformVersion"))).willReturn(givenValues("10.0"));
        given(match.getValues(eq("BrowserName"))).willReturn(givenValues("Edge"));
        given(match.getValues(eq("BrowserVersion"))).willReturn(givenValues("15"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("fiftyonedegrees")
                .deviceType(DeviceType.DESKTOP)
                .deviceTypeRaw("Desktop")
                .osfamily("Windows")
                .os("Windows")
                .osVersion("10.0")
                .browser("Edge")
                .browserVersion("15")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoForSafari() throws IOException, NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) "
                + "AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1";
        given(match.getValues(eq("PlatformName"))).willReturn(givenValues("iOS"));
        given(match.getValues(eq("PlatformVersion"))).willReturn(givenValues("10.3.1"));
        given(match.getValues(eq("BrowserName"))).willReturn(givenValues("Mobile Safari"));
        given(match.getValues(eq("BrowserVersion"))).willReturn(givenValues("10.0"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("fiftyonedegrees")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile")
                .osfamily("iOS")
                .os("iOS")
                .osVersion("10.3.1")
                .browser("Mobile Safari")
                .browserVersion("10.0")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    @Test
    public void getDeviceInfoShouldReturnDeviceInfoForInternetExplorerAndWindowsPhone()
            throws IOException, NoSuchFieldException {
        // given
        givenDownloadedFile();
        final String ua = "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)";
        given(match.getValues(eq("PlatformName"))).willReturn(givenValues("Windows Phone"));
        given(match.getValues(eq("PlatformVersion"))).willReturn(givenValues("7.5"));
        given(match.getValues(eq("BrowserName"))).willReturn(givenValues("Internet Explorer Mobile"));
        given(match.getValues(eq("BrowserVersion"))).willReturn(givenValues("9.0"));

        // when
        final Future<DeviceInfo> resultFuture = deviceInfoService.getDeviceInfo(ua);

        // then
        final DeviceInfo expected = DeviceInfo.builder()
                .vendor("fiftyonedegrees")
                .deviceType(DeviceType.MOBILE)
                .deviceTypeRaw("Mobile")
                .osfamily("Windows Phone")
                .os("Windows Phone")
                .osVersion("7.5")
                .browser("Internet Explorer Mobile")
                .browserVersion("9.0")
                .build();

        assertThat(resultFuture.result()).isEqualTo(expected);
    }

    private void givenDownloadedFile() throws NoSuchFieldException {
        FieldSetter.setField(deviceInfoService,
                deviceInfoService.getClass().getDeclaredField("provider"), provider);
    }

    private Values givenValues(String value) {
        return new Values(property, new Value[]{new Value(null, property, value)});
    }
}
