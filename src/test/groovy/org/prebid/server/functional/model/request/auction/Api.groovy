package org.prebid.server.functional.model.request.auction

import com.fasterxml.jackson.annotation.JsonValue

enum Api {

    VPAID_1(1),
    VPAID_2(2),
    MRAID_1(3),
    ORMMA(4),
    MRAID_2(5),
    MRAID_3(6)

    @JsonValue
    Integer value

    Api(Integer value) {
        this.value = value
    }

    @Override
    String toString() {
        value
    }
}
