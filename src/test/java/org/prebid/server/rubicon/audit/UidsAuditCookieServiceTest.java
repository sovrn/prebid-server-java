package org.prebid.server.rubicon.audit;

import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.rubicon.audit.proto.UidAudit;

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

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

public class UidsAuditCookieServiceTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private RoutingContext routingContext;

    private Cipher decodingCipher;

    private UidsAuditCookieService uidsAuditCookieService;

    @Mock
    private HttpServerRequest request;

    @Before
    public void init() throws InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException {
        uidsAuditCookieService = UidsAuditCookieService.create("key", 10, "127.0.0.1");
        given(routingContext.request()).willReturn(request);
        given(request.getHeader(HttpHeaders.REFERER)).willReturn("referrer");

        // register blowfish decryptor to encode values to check
        decodingCipher = Cipher.getInstance("Blowfish");
        final SecretKeySpec secretKeySpec = new SecretKeySpec("key".getBytes(), "Blowfish");
        decodingCipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
    }

    @Test
    public void createShouldThrowIllegalArgumentExceptionIfCookieEnabledAndKeyIsNull() {
        // given, when and then
        assertThatThrownBy(() -> UidsAuditCookieService.create(null, null, null))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cookies audit encryption cannot be done without encryption key");
    }

    @Test
    public void getUidsAuditShouldReturnExpectedResultIfCookieExists() {
        // given
        // Base64 encoded Blowfish encrypted string - 1|uid^^^^1527684614^referrer|^|1^consent
        given(routingContext.cookieMap())
                .willReturn(singletonMap("uids-audit", Cookie.cookie("uids-audit",
                        "pMmn7z8bUD5mOWCasu0nA42E99uwy-gx6uIWeNPO2UpXs5Lhsj8uS9g992oiVBPB")));

        // when
        final UidAudit uidAudit = uidsAuditCookieService.getUidsAudit(routingContext);

        // then
        assertThat(uidAudit).isEqualTo(UidAudit.builder()
                .version("1")
                .uid("uid")
                .renewedSeconds(1527684614L)
                .referringDomain("referrer")
                .consentUsed("1")
                .consent("consent")
                .build());
    }

    @Test
    public void getUidsAuditShouldFailIfCookieCannotBeParsed() {
        // given
        given(routingContext.cookieMap())
                .willReturn(singletonMap("uids-audit", Cookie.cookie("uids-audit", "invalid")));

        // when and then
        assertThatThrownBy(() -> uidsAuditCookieService.getUidsAudit(routingContext))
                .isExactlyInstanceOf(PreBidException.class);
    }

    @Test
    public void getUidsAuditShouldFailIfCookieCannotBeParsedBecauseOfIllegalBase64Character() {
        // given
        given(routingContext.cookieMap())
                .willReturn(singletonMap("uids-audit", Cookie.cookie("uids-audit", "/contains-illegal-chars/")));

        // when and then
        assertThatThrownBy(() -> uidsAuditCookieService.getUidsAudit(routingContext))
                .isExactlyInstanceOf(PreBidException.class)
                .hasMessage("Illegal base64 character 2f");
    }

    @Test
    public void getUidsAuditShouldReturnNullIfNoCookieExists() {
        // when
        final UidAudit uidAudit = uidsAuditCookieService.getUidsAudit(routingContext);

        // then
        assertThat(uidAudit).isNull();
    }

    @Test
    public void createUidsAuditCookieShouldReturnNewCookie() throws BadPaddingException, IllegalBlockSizeException {
        // given
        given(routingContext.getCookie("uids-audit")).willReturn(null);

        // when
        final Cookie cookie = uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", "accountId",
                "consent",
                "uk",
                "1.2.3.0");

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .startsWith("1|uid^2130706433^16909056^uk^").endsWith("^referrer|^accountId|1^consent");
        assertThat(cookie.getDomain()).isEqualTo("rubiconproject.com");
        assertThat(cookie.encode()).containsSequence("Max-Age=864000; Expires=");
    }

    @Test
    public void createUidsAuditCookieShouldDoIpLookUpIfHostIpIsNullInConfig() throws BadPaddingException,
            IllegalBlockSizeException, SocketException, UnknownHostException {
        // given
        uidsAuditCookieService = UidsAuditCookieService.create("key", 10, null);

        // when
        final Cookie cookie = uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", null, null, null,
                null);

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .contains(convertIpToDecimal(getHostIpFromJvm()));
    }

    @Test
    public void createUidsAuditCookieShouldThrowPrebidExceptionIfUidWasNotDefined() {
        // given
        given(routingContext.getCookie("uids-audit")).willReturn(null);

        // when and then
        assertThatThrownBy(() -> uidsAuditCookieService.createUidsAuditCookie(routingContext, null, null, null, null,
                null))
                .isExactlyInstanceOf(PreBidException.class)
                .hasMessage("Uid was not defined, should be present to set uid audit cookie");
    }

    @Test
    public void createUidsCookieShouldThrowPrebidExceptionIfIpHasIncorrectFormat() {
        // given
        given(routingContext.getCookie("uids-audit")).willReturn(null);

        // when and then
        assertThatThrownBy(() -> uidsAuditCookieService.createUidsAuditCookie(routingContext, "uid", null, null, null,
                "invalidIpFormat"))
                .isExactlyInstanceOf(PreBidException.class)
                .hasMessage("Incorrect ip address format - \"invalidIpFormat\".");
    }

    @Test
    public void updateUidsCookieShouldUpdateReferrerDomain() throws BadPaddingException, IllegalBlockSizeException {
        // given
        given(request.getHeader(HttpHeaders.REFERER)).willReturn("updatedReferrer");
        final UidAudit uidAudit = UidAudit.builder().build();

        // when
        final Cookie cookie = uidsAuditCookieService.updateUidsAuditCookie(routingContext, "consent", uidAudit);

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .endsWith("^updatedReferrer|^|1^consent");
    }

    @Test
    public void updateUidsAuditCookieShouldUpdateConsentFlagAndValueIfItWasAbsent() throws BadPaddingException,
            IllegalBlockSizeException {
        // given
        final UidAudit uidAudit = UidAudit.builder().build();

        // when
        final Cookie cookie = uidsAuditCookieService.updateUidsAuditCookie(routingContext, "consent", uidAudit);

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .endsWith("1^consent");
    }

    @Test
    public void updateUidsAuditCookieShouldNotUpdateConsentIfItHasValueBefore() throws BadPaddingException,
            IllegalBlockSizeException {
        // given
        final UidAudit uidAudit = UidAudit.builder().consent("consent").consentUsed("1").build();

        // when
        final Cookie cookie = uidsAuditCookieService.updateUidsAuditCookie(routingContext, "updatedConsent", uidAudit);

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .endsWith("1^consent");
    }

    @Test
    public void updateUidsAuditCookieShouldReturnZeroFlagIfWithoutConsentIfConsentStringIsNull()
            throws BadPaddingException, IllegalBlockSizeException {
        // given
        final UidAudit uidAudit = UidAudit.builder().consentUsed("0").build();

        // when
        final Cookie cookie = uidsAuditCookieService.updateUidsAuditCookie(routingContext, null, uidAudit);

        // then
        assertThat(new String(decodingCipher.doFinal(Base64.getUrlDecoder().decode(cookie.getValue().getBytes()))))
                .endsWith("|0^");
    }

    private static String getHostIpFromJvm() throws UnknownHostException, SocketException {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        }
    }

    private static String convertIpToDecimal(String ip) throws UnknownHostException {
        return new BigInteger(1, InetAddress.getByName(ip).getAddress()).toString();
    }
}
