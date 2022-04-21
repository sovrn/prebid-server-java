package org.prebid.server.functional.model.bidderspecific

import groovy.transform.ToString
import org.prebid.server.functional.model.request.auction.Device

@ToString(includeNames = true, ignoreNulls = true)
class BidderDevice extends Device {

    DeviceExt ext
}
