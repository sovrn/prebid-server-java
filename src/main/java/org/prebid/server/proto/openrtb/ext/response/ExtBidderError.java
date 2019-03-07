package org.prebid.server.proto.openrtb.ext.response;

import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.Set;

/**
 * Stores error information: error code and message.
 */
@AllArgsConstructor(staticName = "of")
@Value
public class ExtBidderError {

    int code;

    String message;

    Set<String> impIds;

    public static ExtBidderError of(int code, String message) {
        return ExtBidderError.of(code, message, null);
    }
}
