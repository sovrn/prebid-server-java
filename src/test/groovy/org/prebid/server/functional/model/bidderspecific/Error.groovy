package org.prebid.server.functional.model.bidderspecific

import groovy.transform.ToString

@ToString(includeNames = true, ignoreNulls = true)
class Error {

    String code
    String desc
}
