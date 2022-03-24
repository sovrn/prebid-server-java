package org.prebid.server.functional.model.mock.services.rubiconanalytics

import com.fasterxml.jackson.annotation.JsonProperty
import groovy.transform.ToString

@ToString(includeNames = true, ignoreNulls = true)
class BidResponseBid {

    @JsonProperty("bidPriceUSD")
    Float bidPriceUsd
    String mediaType
    BigDecimal floorValue
    String floorRule
    BigDecimal floorRuleValue
    String dealId
}
