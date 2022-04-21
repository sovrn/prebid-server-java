package org.prebid.server.functional.model.bidderspecific

import groovy.transform.ToString
import org.prebid.server.functional.model.request.auction.Publisher

@ToString(includeNames = true, ignoreNulls = true)
class PublisherExt extends Publisher {

    Rp rp
}
