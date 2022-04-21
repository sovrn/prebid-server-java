package org.prebid.server.functional.model.bidderspecific

import groovy.transform.ToString
import org.prebid.server.functional.model.request.auction.Site

@ToString(includeNames = true, ignoreNulls = true)
class BidderSite extends Site {

    BidderPublisher publisher
    BidderSiteExt ext
}
