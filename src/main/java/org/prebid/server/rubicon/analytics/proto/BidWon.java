package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Builder
@Value
public class BidWon {

    @JsonProperty("transactionId")
    String transactionId;

    @JsonProperty("accountId")
    Integer accountId;

    String bidder;

    @JsonProperty("samplingFactor")
    Integer samplingFactor;

    @JsonProperty("bidwonStatus")
    String bidwonStatus;

    Error error;

    @JsonProperty("mediaTypes")
    List<String> mediaTypes;

    @JsonProperty("videoAdFormat")
    String videoAdFormat;

    @JsonProperty("adUnitCode")
    String adUnitCode;

    String source;

    @JsonProperty("serverLatencyMillis")
    Integer serverLatencyMillis;

    @JsonProperty("serverHasUserId")
    Boolean serverHasUserId;

    @JsonProperty("hasRubiconId")
    Boolean hasRubiconId;

    Params params;

    @JsonProperty("bidResponse")
    BidResponse bidResponse;
}
