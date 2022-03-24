package org.prebid.server.functional.model.mock.services.rubiconanalytics

import com.fasterxml.jackson.annotation.JsonProperty
import groovy.transform.ToString

@ToString(includeNames = true, ignoreNulls = true)
class AdUnit {

    String transactionId
    String status
    List<String> mediaTypes
    List<Dimension> dimensions
    List<Bid> bids
    Integer siteId
    Integer zoneId
    String adUnitCode
    @JsonProperty("adserverTargeting")
    Map<String, String> adServerTargeting
}
