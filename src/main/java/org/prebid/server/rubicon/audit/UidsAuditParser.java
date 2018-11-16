package org.prebid.server.rubicon.audit;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.rubicon.audit.proto.UidAudit;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Class providing methods to parse audit cookie value.
 */
public class UidsAuditParser {

    public static final String VERSION = "1";

    private static final String GROUP_DELIMITER = "|";
    private static final String FIELD_DELIMITER = "^";
    private static final String QUOTED_GROUP_DELIMITER = Pattern.quote(GROUP_DELIMITER);
    private static final String QUOTED_FIELD_DELIMITER = Pattern.quote(FIELD_DELIMITER);
    private static final String CONSENT_USED = "1";
    private static final String CONSENT_WAS_NOT_USED = "0";
    private static final int CONSENT_TRIM_LENGTH = 100;

    // set String.split() limit to -1 allows to get empty fields from missed value between delimiters.
    private static final int SPLIT_LIMIT_MINUS_ONE = -1;

    private UidsAuditParser() {
    }

    /**
     * Parsed audit cookie value and mapped it to {@link UidAudit} object.
     */
    static UidAudit parseCookieValue(String auditCookieValue) throws InvalidAuditFormatException {
        final List<String> auditGroups = Arrays.asList(auditCookieValue.split(QUOTED_GROUP_DELIMITER,
                SPLIT_LIMIT_MINUS_ONE));
        final String version = auditGroups.get(0);

        final UidAudit uidAudit;

        // algorithm to parse cookie depends on version
        if (Objects.equals(version, "1")) {
            uidAudit = parseFirstVersionCookie(auditGroups);
        } else {
            throw new InvalidAuditFormatException(String.format(
                    "Uids audit cookie has invalid version = %s", version));
        }
        return uidAudit;
    }

    /**
     * Implements algorithm for parsing audit cookie value for first algorithm version. Maps parsed value to
     * {@link UidAudit} object.
     */
    private static UidAudit parseFirstVersionCookie(List<String> auditGroups) throws InvalidAuditFormatException {
        final UidAudit.UidAuditBuilder builder = UidAudit.builder();

        final int groupsNumber = auditGroups.size();
        if (groupsNumber != 4) {
            throw new InvalidAuditFormatException(String.format("Expected groups number for cookie structure version #1"
                    + " is 4, but actually was %s", groupsNumber));
        }

        // group 1
        builder.version(auditGroups.get(0));

        // group 2
        final String[] secondGroupFields = auditGroups.get(1).split(QUOTED_FIELD_DELIMITER, SPLIT_LIMIT_MINUS_ONE);

        // expected group 2 has 6 fields
        validateGroupSize(2, 6, secondGroupFields.length);
        final String renewedSeconds = firstNotEmpty(secondGroupFields[4]);
        builder.uid(firstNotEmpty(secondGroupFields[0]))
                .hostIp(firstNotEmpty(secondGroupFields[1]))
                .userIp(firstNotEmpty(secondGroupFields[2]))
                .country(firstNotEmpty(secondGroupFields[3]))
                .renewedSeconds(renewedSeconds != null ? Long.parseLong(renewedSeconds) : null)
                .referringDomain(firstNotEmpty(secondGroupFields[5]));

        // group 3
        final String[] thirdGroupFields = auditGroups.get(2).split(QUOTED_FIELD_DELIMITER, SPLIT_LIMIT_MINUS_ONE);

        // expected group 3 has 2 fields
        validateGroupSize(3, 2, thirdGroupFields.length);
        builder.initiatorType(firstNotEmpty(thirdGroupFields[0])).initiatorId(firstNotEmpty(thirdGroupFields[1]));

        // group 4
        final String[] fourthGroupFields = auditGroups.get(3).split(QUOTED_FIELD_DELIMITER, SPLIT_LIMIT_MINUS_ONE);

        // expected group 4 has 2 fields
        validateGroupSize(4, 2, fourthGroupFields.length);
        final String isConsentUsed = fourthGroupFields[0];
        if (isConsentUsedValid(isConsentUsed)) {
            builder.consentUsed(isConsentUsed);
            builder.consent(firstNotEmpty(fourthGroupFields[1]));
        } else {
            throw new InvalidAuditFormatException(String.format("Consent flag should be present and equals to 0 or 1,"
                    + " but actually was \"%s\"", isConsentUsed));
        }
        return builder.build();
    }

    private static void validateGroupSize(int groupNumber, int expectedSize, int actualSize)
            throws InvalidAuditFormatException {
        if (expectedSize != actualSize) {
            throw new InvalidAuditFormatException(
                    String.format("Expected group fields number for group #%s is %s, but actually was %s", groupNumber,
                            expectedSize, actualSize));
        }
    }

    /**
     * Returns flag if consent used flag is valid
     */
    private static boolean isConsentUsedValid(String isConsentUsed) {
        return StringUtils.isNotEmpty(isConsentUsed) && (isConsentUsed.equals(CONSENT_USED)
                || isConsentUsed.equals(CONSENT_WAS_NOT_USED));
    }

    private static String firstNotEmpty(String value) {
        return StringUtils.isEmpty(value) ? null : value;
    }

    /**
     * Creates string representation of {@link UidAudit}.
     */
    static String uidAuditToRow(UidAudit uidAudit) {
        final StringBuilder uidAuditRowBuilder = new StringBuilder();
        // group 1
        uidAuditRowBuilder.append(uidAudit.getVersion()).append(GROUP_DELIMITER)
                // group 2
                .append(ObjectUtils.firstNonNull(uidAudit.getUid(), "")).append(FIELD_DELIMITER)
                .append(ObjectUtils.firstNonNull(uidAudit.getHostIp(), "")).append(FIELD_DELIMITER)
                .append(ObjectUtils.firstNonNull(uidAudit.getUserIp(), "")).append(FIELD_DELIMITER)
                .append(ObjectUtils.firstNonNull(uidAudit.getCountry(), "")).append(FIELD_DELIMITER)
                .append(ObjectUtils.firstNonNull(uidAudit.getRenewedSeconds(), "")).append(FIELD_DELIMITER)
                .append(ObjectUtils.firstNonNull(uidAudit.getReferringDomain(), "")).append(GROUP_DELIMITER)
                // group 3
                .append(ObjectUtils.firstNonNull(uidAudit.getInitiatorType(), "")).append(FIELD_DELIMITER)
                .append(ObjectUtils.firstNonNull(uidAudit.getInitiatorId(), "")).append(GROUP_DELIMITER)
                // group 4
                .append(uidAudit.getConsentUsed()).append(FIELD_DELIMITER);
        if (uidAudit.getConsentUsed().equals(CONSENT_USED)) {
            final String consent = ObjectUtils.firstNonNull(uidAudit.getConsent(), "");
            uidAuditRowBuilder.append(consent != null && consent.length() > CONSENT_TRIM_LENGTH
                    ? consent.substring(0, CONSENT_TRIM_LENGTH)
                    : consent);
        }
        return uidAuditRowBuilder.toString();
    }

    /**
     * Masks and converts to decimal format ipv4 or ipv6. In case of incorrect ip format throws {@link PreBidException}.
     */
    static String getMaskedDecimalIp(String ip) throws InvalidAuditFormatException {
        // skip ip in audit cookie if not found
        if (ip == null) {
            return "";
        }
        final String maskedIp;
        if (InetAddressValidator.getInstance().isValidInet4Address(ip)) {
            // mask 1 byte for ipv4
            maskedIp = ipToDecimal(maskIp(ip, '.'));
        } else if (InetAddressValidator.getInstance().isValidInet6Address(ip)) {
            // mask 2 bytes for ipv6
            maskedIp = ipToDecimal(maskIp(ip, ':'));
        } else {
            throw new InvalidAuditFormatException(String.format("Incorrect ip address format - \"%s\".", ip));
        }
        return maskedIp;
    }

    /**
     * Set to zero all bits after the last ip delimiter.
     */
    private static String maskIp(String ip, char delimiter) {
        final int lastIndexOfDelimiter = ip.lastIndexOf(delimiter);
        return ip.substring(0, lastIndexOfDelimiter + 1)
                + StringUtils.repeat('0', ip.length() - lastIndexOfDelimiter - 1);
    }

    /**
     * Converts ipv4 or ipv6 to decimal format.
     */
    static String ipToDecimal(String ip) throws InvalidAuditFormatException {
        final InetAddress inetAddress;
        try {
            inetAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            throw new InvalidAuditFormatException(String.format("Incorrect ip address format - '%s'.", ip));
        }
        return new BigInteger(1, inetAddress.getAddress()).toString();
    }

}
