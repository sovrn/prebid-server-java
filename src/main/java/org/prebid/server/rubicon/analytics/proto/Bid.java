package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class Bid {

    String bidder;

    String status;

    BidError error;

    String source;

    @JsonProperty("serverLatencyMillis")
    Integer serverLatencyMillis;

    @JsonProperty("serverHasUserId")
    Boolean serverHasUserId;

    Params params;

    @JsonProperty("bidResponse")
    BidResponse bidResponse;
}
