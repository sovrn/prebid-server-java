package org.prebid.server.functional.model.mock.services.rubiconanalytics

import groovy.transform.ToString

@ToString(includeNames = true, ignoreNulls = true)
class Auction {

    String requestId
    Integer samplingFactor
    List<AdUnit> adUnits
    String accountId
    Integer serverTimeoutMillis
    Boolean hasRubiconId
    Gdpr gdpr
}
