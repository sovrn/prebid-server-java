package org.prebid.server.functional.model.mock.services.rubiconanalytics

import com.fasterxml.jackson.annotation.JsonValue
import groovy.transform.ToString

@ToString
enum Status {

    SUCCESS("success"),
    NO_BID("no-bid"),
    ERROR("error"),
    REJECTED("rejected"),
    REJECTED_IPF("rejected-ipf"),
    REJECTED_GDPR("rejected-gdpr")

    @JsonValue
    final String value

    Status(String value) {
        this.value = value
    }

    @Override
    String toString() {
        value
    }
}
