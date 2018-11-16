package org.prebid.server.rubicon.rsid;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Cookie;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.rubicon.rsid.model.Rsid;

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
    public Rsid parseFromRequest(RoutingContext context) {
        final Cookie rsidCookie = context.getCookie(COOKIE_NAME);
        final String rsidRawValue = rsidCookie != null ? rsidCookie.getValue() : null;

        return rsidRawValue != null ? toRsid(rsidRawValue) : null;
    }

    /**
     * 1. Decodes input string from Base64.
     * <p>
     * 2. Decrypts decoded value to RSID string.
     * <p>
     * 3. Creates {@link Rsid} result model.
     */
    private Rsid toRsid(String rsidRawValue) {
        final byte[] rsidBytes;
        try {
            rsidBytes = Base64.getDecoder().decode(rsidRawValue);
        } catch (IllegalArgumentException e) {
            logger.warn("Cannot decode RSID from: {0}", e, rsidRawValue);
            return null;
        }

        final StringBuilder sb = new StringBuilder();
        decrypt(rsidBytes, 0, encryptionKey.getBytes(), 0, sb);

        final String[] groups = sb.toString().split("\\^");
        final String country = groups.length > 3 ? StringUtils.stripToNull(groups[3]) : null;
        return Rsid.of(country);
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
