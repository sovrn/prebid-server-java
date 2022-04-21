package org.prebid.server.functional.model.bidderspecific

import groovy.transform.ToString
import org.prebid.server.functional.model.request.auction.SiteExt

@ToString(includeNames = true, ignoreNulls = true)
class BidderSiteExt extends SiteExt {

    Rp rp
}
