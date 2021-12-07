package org.prebid.server.rubicon.audit.proto;

import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class UidAudit {

    // group 1

    /**
     * Cookie structure version.
     */
    String version;

    // group 2

    /**
     * Audited uid cookie value.
     */
    String uid;

    /**
     * Host's IP address – encapsulates the source system and data center.
     */
    String hostIp;

    /**
     * User's IP address with masked last octet.
     */
    String userIp;

    /**
     * Country obtained user ip address in ISO-3166-1-alpha-2 format.
     */
    String country;

    /**
     * UTC time the most recent time the cookie was renewed in seconds.
     */
    Long renewedSeconds;

    /**
     * The referring domain at the time consent was renewed.
     */
    String referringDomain;

    // group 3

    /**
     * Initiator type – e.g., partner, account, integration.
     */
    String initiatorType;

    /**
     * Initiator ID – e.g., the REVV partner_id, account_id or integration name.
     */
    String initiatorId;

    // group 4

    /**
     * Flag to indicate a consent string was used, 0 - false, 1 - true.
     */
    String consentUsed;

    /**
     * The consent string itself – consider truncating to some number of characters.
     */
    String consent;
}
