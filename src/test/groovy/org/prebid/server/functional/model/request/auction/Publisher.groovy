package org.prebid.server.functional.model.request.auction

import groovy.transform.ToString
import org.prebid.server.functional.model.bidderspecific.PublisherExt
import org.prebid.server.functional.util.PBSUtils

@ToString(includeNames = true, ignoreNulls = true)
class Publisher {

    String id
    String name
    List<String> cat
    String domain
    PublisherExt ext

    static Publisher getDefaultPublisher() {
        new Publisher(id: PBSUtils.randomNumber)
    }
}
