package org.prebid.server.functional.model.response.auction

import com.fasterxml.jackson.annotation.JsonValue

enum BidRejectionReason {

    ERROR_NO_BID(0),
    ERROR_GENERAL(100),
    ERROR_TIMED_OUT(101),
    ERROR_INVALID_BID_RESPONSE(102),
    ERROR_BIDDER_UNREACHABLE(103),

    REQUEST_BLOCKED_GENERAL(200),
    REQUEST_BLOCKED_UNSUPPORTED_CHANNEL(201),
    REQUEST_BLOCKED_UNSUPPORTED_MEDIA_TYPE(202),
    REQUEST_BLOCKED_PRIVACY(204),
    REQUEST_BLOCKED_UNACCEPTABLE_CURRENCY(205),

    RESPONSE_REJECTED_GENERAL(300),
    RESPONSE_REJECTED_DUE_TO_PRICE_FLOOR(301),
    RESPONSE_REJECTED_DUE_TO_DSA(305),
    RESPONSE_REJECTED_INVALID_CREATIVE(350),
    RESPONSE_REJECTED_INVALID_CREATIVE_SIZE(351),
    RESPONSE_REJECTED_INVALID_CREATIVE_NOT_SECURE(352),
    RESPONSE_REJECTED_ADVERTISER_BLOCKED(356)

    @JsonValue
    final Integer code

    BidRejectionReason(Integer value) {
        this.code = value
    }
}
