package org.prebid.server.functional.model.mock.services.rubiconanalytics

import groovy.transform.ToString
import org.prebid.server.functional.model.ChannelType

@ToString(includeNames = true, ignoreNulls = true)
class RubiconAnalyticsResponse {

    String integration
    String version
    List<Auction> auctions
    EventCreator eventCreator
    String userAgent
    ChannelType channel
    String referrerUri
}
