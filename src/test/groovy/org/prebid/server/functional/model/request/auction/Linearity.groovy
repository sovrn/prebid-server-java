package org.prebid.server.functional.model.request.auction

import com.fasterxml.jackson.annotation.JsonValue

enum Linearity {

    LINEAR(1),
    NON_LINEAR(2)

    @JsonValue
    Integer value

    Linearity(Integer value) {
        this.value = value
    }

    @Override
    String toString() {
        value
    }
}
