package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Impression {

    @JsonProperty("accountId")
    Integer accountId;

    String bidder;

    String status;

    String source;

    @JsonProperty("bidId")
    String bidId;

    @JsonProperty("serverHasUserId")
    Boolean serverHasUserId;

    @JsonProperty("hasRubiconId")
    Boolean hasRubiconId;
}
