package org.prebid.server.rubicon.analytics.proto;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class BidError {

    public static final String REQUEST_ERROR = "request-error";
    public static final String TIMEOUT_ERROR = "timeout-error";

    String code;

    String description;

    public static BidError requestError(String description) {
        return BidError.of(REQUEST_ERROR, description);
    }

    public static BidError timeoutError(String description) {
        return BidError.of(TIMEOUT_ERROR, description);
    }
}
