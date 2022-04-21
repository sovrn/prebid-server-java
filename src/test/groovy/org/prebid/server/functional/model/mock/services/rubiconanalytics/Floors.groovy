package org.prebid.server.functional.model.mock.services.rubiconanalytics

import groovy.transform.ToString
import org.prebid.server.functional.model.request.auction.FetchStatus
import org.prebid.server.functional.model.request.auction.Location

@ToString(includeNames = true, ignoreNulls = true)
class Floors {

    Location location
    FetchStatus fetchStatus
    Boolean skipped
    String modelName
    Boolean enforcement
    Boolean dealsEnforced
    Integer skipRate
    String provider
    BigDecimal floorMin
    Integer modelWeight
    Long modelTimestamp
}
