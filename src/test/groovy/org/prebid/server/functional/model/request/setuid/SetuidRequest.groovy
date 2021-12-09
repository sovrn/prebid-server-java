package org.prebid.server.functional.model.request.setuid

import com.fasterxml.jackson.annotation.JsonProperty
import groovy.transform.ToString
import org.prebid.server.functional.model.bidder.BidderName
import org.prebid.server.functional.model.request.Format

import static org.prebid.server.functional.model.bidder.BidderName.GENERIC
import static org.prebid.server.functional.model.bidder.BidderName.RUBICON

@ToString(includeNames = true, ignoreNulls = true)
class SetuidRequest {

    BidderName bidder
    String uid
    String gdpr
    @JsonProperty("gdpr_consent")
    String gdprConsent
    @JsonProperty("f")
    Format format
    String account

    static SetuidRequest getDefaultSetuidRequest() {
       new SetuidRequest(bidder: GENERIC, gdpr: "0")
    }

    static SetuidRequest getRubiconSetuidRequest() {
        new SetuidRequest(bidder: RUBICON, gdpr: "0")
    }
}
