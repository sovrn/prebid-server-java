package org.prebid.server.functional.model.bidder

import com.fasterxml.jackson.annotation.JsonValue

enum BidderName {

    GENERIC("generic"),
    RUBICON("rubicon"),
    APPNEXUS("appnexus"),
    RUBICON_ALIAS("rubiconAlias")

    @JsonValue
    final String value

    BidderName(String value) {
        this.value = value
    }

    String toString() {
        value
    }
}
