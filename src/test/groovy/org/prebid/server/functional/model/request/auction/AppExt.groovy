package org.prebid.server.functional.model.request.auction

import groovy.transform.ToString
import org.prebid.server.functional.model.bidderspecific.Rp

@ToString(includeNames = true, ignoreNulls = true)
class AppExt {

    AppExtData data
    Rp rp
}
