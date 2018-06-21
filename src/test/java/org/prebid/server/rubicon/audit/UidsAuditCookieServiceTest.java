package org.prebid.server.rubicon.audit;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.Cookie;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.exception.PreBidException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

public class UidsAuditCookieServiceTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerRequest request;

    @Mock
    private Cookie uidAuditCookie;

    private Cipher decodingCipher;

    private UidsAuditCookieService uidsAuditCookieService;

    @Before
    public void init() throws InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException {
        uidsAuditCookieService = UidsAuditCookieService.create("key", 10, "127.0.0.1");
        given(routingContext.request()).willReturn(request);
        given(request.getHeader(HttpHeaders.REFERER)).willReturn("referrer");

        // register blowfish decriptor to encode values to check
        decodingCipher = Cipher.getInstance("Blowfish");
        final SecretKeySpec secretKeySpec = new SecretKeySpec("key".getBytes(), "Blowfish");
        decodingCipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
    }

    @Test
    public void createShouldThrowIllegalArgumentExceptionIfCookieEnabledAndKeyIsNull() {
        // given, when and then
        assertThatThrownBy(() -> UidsAuditCookieService.create(null, null, null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cookies audit encryption cannot be done without encryption key.");

    }

    @Test
    public void createUidsAuditCookieShouldReturnNewCookie() throws BadPaddingException, IllegalBlockSizeException {
        // given
        given(routingContext.getCookie("audit")).willReturn(null);

        // when
        final Cookie cookie = uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", "accountId", "consent",
                "uk",
                "1.2.3.0");

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .startsWith("1|uid^2130706433^16909056^uk^").endsWith("^referrer|^accountId|1^consent");
        assertThat(cookie.getDomain()).isEqualTo("rubiconproject.com");
        assertThat(cookie.encode()).containsSequence("Max-Age=864000; Expires=");
    }

    @Test
    public void createUidsCookieShouldUpdateReferrerDomain() throws BadPaddingException, IllegalBlockSizeException {
        // given
        given(routingContext.getCookie("audit")).willReturn(uidAuditCookie);
        // Base64 encoded Blowfish encrypted string - 1|uid^^^^1527684614^referrer|^|1^consent
        given(uidAuditCookie.getValue()).willReturn("pMmn7z8bUD5mOWCasu0nA42E99uwy-gx6uIWeNPO2UpXs5Lhsj8uS9g992oiVBPB");
        given(request.getHeader(HttpHeaders.REFERER)).willReturn("updatedReferrer");

        // when
        final Cookie cookie = uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", null, "consent", "uk",
                "1.2.3.0");

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .endsWith("^updatedReferrer|^|1^consent");
    }

    @Test
    public void createUidsAuditCookieShouldUpdateConsentFlagAndValueIfItWasAbsent() throws BadPaddingException,
            IllegalBlockSizeException {
        // given
        given(routingContext.getCookie("audit")).willReturn(uidAuditCookie);
        // Base64 encoded Blowfish encrypted string - 1|uid^^^^1527684614^referrer|^|0^
        given(uidAuditCookie.getValue()).willReturn("pMmn7z8bUD5mOWCasu0nA42E99uwy-gx__Eaz45aJobTyHZ2tZJZzg==");

        // when
        final Cookie cookie = uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", null, "consent", "uk",
                "1.2.3.0");

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .endsWith("1^consent");
    }

    @Test
    public void createUidsAuditCookieShouldNotUpdateConsentIfItHasValueBefore() throws BadPaddingException,
            IllegalBlockSizeException {
        // given
        given(routingContext.getCookie("audit")).willReturn(uidAuditCookie);
        // Base64 encoded Blowfish encrypted string - 1|uid^^^^^|^|1^consent
        given(uidAuditCookie.getValue()).willReturn("pMmn7z8bUD5RyvzuB-VDucAgztAL2CGK");

        // when
        final Cookie cookie = uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", null, "updatedConsent",
                null, null);

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .endsWith("1^consent");
    }

    @Test
    public void createUidsAuditCookieShouldReturnZeroFlagIfWithoutConsentIfConsentStringIsNull()
            throws BadPaddingException, IllegalBlockSizeException {
        // given
        given(routingContext.getCookie("audit")).willReturn(null);

        // when
        final Cookie cookie = uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", null,  null, null, null);

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .endsWith("|0^");
    }

    @Test
    public void createUidsAuditCookieShouldDoIpLookUpIfHostIdIsNullInConfig() throws BadPaddingException,
            IllegalBlockSizeException, SocketException, UnknownHostException {
        // given
        given(routingContext.getCookie("audit")).willReturn(null);

        uidsAuditCookieService = UidsAuditCookieService.create("key", 10, null);

        // when
        final Cookie cookie = uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", null, null, null, null);

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .contains(convertIpToDecimal(getHostIpFromJvm()));
    }

    @Test
    public void createUidsAuditCookieShouldThrowPrebidExceptionIfUidWasNotDefined() throws BadPaddingException,
            IllegalBlockSizeException {
        // given
        given(routingContext.getCookie("audit")).willReturn(null);

        // when and then
        assertThatThrownBy(() -> uidsAuditCookieService.createUidsAuditCookie(routingContext, null, null, null, null,
                null))
                .isExactlyInstanceOf(PreBidException.class)
                .hasMessage("Uid was not defined. Should be present to set uid audit cookie.");
    }

    @Test
    public void createUidsCookieShouldThrowPrebidExceptionIfIpHasIncorrectFormat() {
        // given
        given(routingContext.getCookie("audit")).willReturn(null);

        // when and then
        assertThatThrownBy(() -> uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", null, null, null,
                "invalidIpFormat"))
                .isExactlyInstanceOf(PreBidException.class)
                .hasMessage("Incorrect ip address format - \"invalidIpFormat\".");
    }

    private static String getHostIpFromJvm() throws UnknownHostException, SocketException {
        try (final DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        }
    }

    private static String convertIpToDecimal(String ip) throws UnknownHostException {
        return new BigInteger(1, InetAddress.getByName(ip).getAddress()).toString();
    }
}
