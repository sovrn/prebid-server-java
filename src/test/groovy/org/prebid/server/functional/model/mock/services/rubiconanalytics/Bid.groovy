package org.prebid.server.functional.model.mock.services.rubiconanalytics

import groovy.transform.ToString
import org.prebid.server.functional.model.bidder.Rubicon

@ToString(includeNames = true, ignoreNulls = true)
class Bid {

    String bidId
    String bidder
    Status status
    String source
    Integer serverLatencyMillis
    Boolean serverHasUserId
    Rubicon params
    BidResponseBid bidResponse
    String bidderDetail
}
