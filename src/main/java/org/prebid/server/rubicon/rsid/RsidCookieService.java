package org.prebid.server.rubicon.rsid;

import io.vertx.core.http.Cookie;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.model.HttpRequestContext;
import org.prebid.server.rubicon.rsid.model.Rsid;
import org.prebid.server.util.HttpUtil;

import java.util.Base64;
import java.util.Objects;

/**
 * Works with Rubicon specific RSID cookie.
 */
public class RsidCookieService {

    private static final Logger logger = LoggerFactory.getLogger(RsidCookieService.class);

    private static final String COOKIE_NAME = "rsid";

    private final String encryptionKey;

    public RsidCookieService(String encryptionKey) {
        this.encryptionKey = Objects.requireNonNull(encryptionKey);
    }

    /**
     * Returns {@link Rsid} from Http cookie in request or null if absent.
     */
    public Rsid parseFromRequest(RoutingContext routingContext) {
        final Cookie rsidCookie = routingContext.cookieMap().get(COOKIE_NAME);
        final String rsidRawValue = rsidCookie != null ? rsidCookie.getValue() : null;

        return rsidRawValue != null ? toRsid(rsidRawValue) : null;
    }

    public Rsid parseFromRequest(HttpRequestContext httpRequest) {
        final String rsidCookieValue = HttpUtil.cookiesAsMap(httpRequest).get(COOKIE_NAME);

        return rsidCookieValue != null ? toRsid(rsidCookieValue) : null;
    }

    /**
     * 1. Pre-process input string.
     * <p>
     * 2. Decodes input string from Base64.
     * <p>
     * 3. Decrypts decoded value to RSID string.
     * <p>
     * 4. Creates {@link Rsid} result model.
     */
    private Rsid toRsid(String rsidRawValue) {
        final String processedRsidValue = preProcessRsidValue(rsidRawValue);
        final byte[] rsidBytes;
        try {
            rsidBytes = Base64.getDecoder().decode(processedRsidValue);
        } catch (IllegalArgumentException e) {
            logger.warn("Cannot decode RSID with error: {0} from: {1}", e.getMessage(), processedRsidValue);
            return null;
        }

        final StringBuilder sb = new StringBuilder();
        decrypt(rsidBytes, 0, encryptionKey.getBytes(), 0, sb);

        final String[] groups = sb.toString().split("\\^");
        final String country = groups.length > 3 ? StringUtils.stripToNull(groups[3]) : null;
        return Rsid.of(country);
    }

    /**
     * Checks if raw RSID string contains invalid character.
     * If it does - excludes that char as well as all characters before it.
     */
    private static String preProcessRsidValue(String rsidRawValue) {
        final int invalidCharIndex = rsidRawValue.indexOf('|');
        return invalidCharIndex == -1 ? rsidRawValue : rsidRawValue.substring(invalidCharIndex + 1);
    }

    /**
     * Decrypts input byte array regarding to the given key byte array.
     */
    private static void decrypt(byte[] input, int inputPtr, byte[] key, int keyPtr, StringBuilder sb) {
        if (inputPtr < input.length && keyPtr < key.length) {
            final int decrypted = input[inputPtr] ^ key[keyPtr] ^ (key[0] * keyPtr);
            final byte value = (byte) decrypted;
            sb.append((char) value);

            key[keyPtr] += keyPtr < (key.length - 1) ? key[keyPtr + 1] : key[0];
            if (key[keyPtr] == 0) {
                key[keyPtr]++;
            }
            final int updatedKeyPtr = keyPtr >= key.length - 1 ? 0 : keyPtr + 1;

            decrypt(input, inputPtr + 1, key, updatedKeyPtr, sb);
        }
    }
}
