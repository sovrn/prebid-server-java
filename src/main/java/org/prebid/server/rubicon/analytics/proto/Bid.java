package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class Bid {

    String bidder;

    String status;

    @JsonProperty("bidSource")
    String bidSource;

    @JsonProperty("bidResponse")
    BidResponse bidResponse;
}
