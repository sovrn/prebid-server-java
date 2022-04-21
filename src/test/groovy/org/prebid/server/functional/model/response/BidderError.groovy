package org.prebid.server.functional.model.response

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming
import groovy.transform.ToString

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy)
@ToString(includeNames = true, ignoreNulls = true)
class BidderError {

    Integer code
    String message
    List<String> impIds
}
