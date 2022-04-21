package org.prebid.server.functional.model.bidderspecific

import groovy.transform.ToString
import org.prebid.server.functional.model.request.auction.Banner

@ToString(includeNames = true, ignoreNulls = true)
class BidderBanner extends Banner {

    BannerExt ext
}
