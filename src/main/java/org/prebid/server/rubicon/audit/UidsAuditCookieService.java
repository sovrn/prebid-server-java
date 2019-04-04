package org.prebid.server.rubicon.audit;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Cookie;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.rubicon.audit.proto.UidAudit;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Map;

/**
 * Service provides uid audit cookie support.
 */
public class UidsAuditCookieService {

    private static final Logger logger = LoggerFactory.getLogger(UidsAuditCookieService.class);

    private static final String COOKIE_NAME = "uids-audit";
    private static final String COOKIE_DOMAIN = "rubiconproject.com";
    private static final String ENCRYPTION_ALGORITHM = "Blowfish";

    private final Cipher encryptor;
    private final Cipher decryptor;
    private final Long ttlSeconds;
    private final String hostIp;

    private UidsAuditCookieService(Cipher encryptor, Cipher decryptor, Long ttlSeconds, String hostIp) {
        this.encryptor = encryptor;
        this.decryptor = decryptor;
        this.ttlSeconds = ttlSeconds;
        this.hostIp = hostIp;
    }

    /**
     * Creates {@link UidsAuditCookieService} by creating blowfish encoder and decoder, and looking for host ip
     * address.
     */
    public static UidsAuditCookieService create(String encryptionKey, Integer ttlDays, String hostIp) {
        if (StringUtils.isEmpty(encryptionKey)) {
            throw new IllegalArgumentException("Cookies audit encryption cannot be done without encryption key");
        }

        final SecretKeySpec secretKeySpec = new SecretKeySpec(encryptionKey.getBytes(), ENCRYPTION_ALGORITHM);
        final Cipher encodingCipher = createCypher(Cipher.ENCRYPT_MODE, secretKeySpec);
        final Cipher decodingCipher = createCypher(Cipher.DECRYPT_MODE, secretKeySpec);
        final String resolvedHostIp;
        if (hostIp == null) {
            logger.warn("Host ip config was not defined in configuration. Will try to find host ip with look up");
            resolvedHostIp = lookUpForHostIp();
        } else {
            resolvedHostIp = hostIp;
        }
        return new UidsAuditCookieService(encodingCipher, decodingCipher, Duration.ofDays(ttlDays).getSeconds(),
                getDecimalIp(resolvedHostIp));
    }

    /**
     * Return ip in decimal format.
     * <p>
     * Throws {@link PreBidException} in case any format exception occurred during converting ip address.
     */
    private static String getDecimalIp(String ip) {
        try {
            return UidsAuditParser.ipToDecimal(ip);
        } catch (InvalidAuditFormatException e) {
            throw new PreBidException(e.getMessage());
        }
    }

    /**
     * Performing external interface ip look up using JVM tools.
     */
    private static String lookUpForHostIp() {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        } catch (SocketException | UnknownHostException e) {
            throw new PreBidException("Host ip address is not defined in configuration and cannot be found by JVM", e);
        }
    }

    /**
     * Creates Blowfish ECB algorithm with PKCS5 padding cypher from mode and secret key.
     */
    private static Cipher createCypher(int mode, SecretKeySpec secretKeySpec) {
        final Cipher blowfishCipher;
        try {
            blowfishCipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
            blowfishCipher.init(mode, secretKeySpec);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException e) {
            logger.error("UidsAuditCookieService was not created: {0}", e, e.getMessage());
            throw new IllegalArgumentException(e.getMessage());
        }
        return blowfishCipher;
    }

    /**
     * Returns {@link UidAudit} from {@link RoutingContext} or null if no audit cookie exists.
     */
    public UidAudit getUidsAudit(RoutingContext context) {
        final Cookie uidAuditCookie = context.getCookie(COOKIE_NAME);
        return uidAuditCookie != null ? getUidAudit(uidAuditCookie.getValue()) : null;
    }

    /**
     * Returns {@link UidAudit} from cookies' map or null if no audit cookie exists.
     */
    public UidAudit getUidsAudit(Map<String, String> cookies) {
        final String uidAuditCookieValue = cookies.get(COOKIE_NAME);
        return getUidAudit(uidAuditCookieValue);
    }

    private UidAudit getUidAudit(String uidAuditCookieValue) {
        try {
            final String decryptedValue = uidAuditCookieValue != null ? decrypt(uidAuditCookieValue) : null;
            return decryptedValue != null ? UidsAuditParser.parseCookieValue(decryptedValue) : null;
        } catch (InvalidAuditFormatException e) {
            return null;
        }
    }

    /**
     * Creates uid audit {@link Cookie} using blowfish algorithm for data encryption and encrypt it with Base64.
     */
    public Cookie createUidsAuditCookie(RoutingContext context, String uid, String accountId, String consent,
                                        String country, String userIp) {
        if (uid == null) {
            throw new PreBidException("Uid was not defined, should be present to set uid audit cookie");
        }

        final String referrer = context.request().getHeader(HttpHeaders.REFERER);
        final long renewedSeconds = ZonedDateTime.now(Clock.systemUTC()).toEpochSecond();

        final UidAudit uidAudit = UidAudit.builder()
                .version(UidsAuditParser.VERSION)
                .uid(uid)
                .hostIp(hostIp)
                .userIp(getMaskedIp(userIp))
                .country(country)
                .renewedSeconds(renewedSeconds)
                .referringDomain(referrer)
                .initiatorType(getInitiatorType())
                .initiatorId(accountId)
                .consentUsed(consent != null ? "1" : "0")
                .consent(consent)
                .build();

        return toCookie(uidAudit);
    }

    /**
     * Updates existing uid audit {@link Cookie}.
     */
    public Cookie updateUidsAuditCookie(RoutingContext context, String consent, UidAudit previousUidAudit) {
        final String referrer = context.request().getHeader(HttpHeaders.REFERER);
        final long renewedSeconds = ZonedDateTime.now(Clock.systemUTC()).toEpochSecond();

        final String previousConsent = previousUidAudit.getConsent();
        final String previousConsentUsed = previousUidAudit.getConsentUsed();
        final boolean updateConsent = previousConsent == null && consent != null;

        final UidAudit uidAudit = previousUidAudit.toBuilder()
                .referringDomain(referrer)
                .renewedSeconds(renewedSeconds)
                .consentUsed(updateConsent ? "1" : previousConsentUsed)
                .consent(updateConsent ? consent : previousConsent)
                .build();

        return toCookie(uidAudit);
    }

    /**
     * Returns masked and converted ip address.
     * <p>
     * Throws {@link PreBidException} in case of any format exception occurred during converting ip address.
     */
    private static String getMaskedIp(String ip) {
        try {
            return UidsAuditParser.getMaskedDecimalIp(ip);
        } catch (InvalidAuditFormatException e) {
            throw new PreBidException(e.getMessage());
        }
    }

    /**
     * Return initiator type.
     */
    private static String getInitiatorType() {
        return null;
    }

    /**
     * Performs blowfish decryption of Base64 encoded value.
     */
    private String decrypt(String encodedValue) {
        try {
            return new String(decryptor.doFinal(Base64.getUrlDecoder().decode(encodedValue.getBytes())));
        } catch (IllegalArgumentException | IllegalBlockSizeException | BadPaddingException e) {
            logger.warn("Error occurred during decoding cookie: {0}", e.getMessage());
            throw new PreBidException(e.getMessage());
        }
    }

    /**
     * Performs blowfish value encryption, with Base64 result encoding.
     */
    private String encrypt(String value) {
        try {
            return Base64.getUrlEncoder().encodeToString(encryptor.doFinal(value.getBytes()));
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            logger.warn("Error occurred during encoding cookie: {0}", e.getMessage());
            throw new PreBidException(e.getMessage());
        }
    }

    /**
     * Creates HTTP Cookie from {@link UidAudit}.
     */
    private Cookie toCookie(UidAudit uidAudit) {
        final String uidAuditRow = encrypt(UidsAuditParser.uidAuditToRow(uidAudit));
        return Cookie.cookie(COOKIE_NAME, uidAuditRow).setDomain(COOKIE_DOMAIN).setMaxAge(ttlSeconds);
    }
}
