package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

@Value
@Builder
public class BidResponse {

    @JsonProperty("dealId")
    String dealId;

    @JsonProperty("bidPriceUSD")
    BigDecimal bidPriceUsd;

    @JsonProperty("mediaType")
    String mediaType;

    Dimensions dimensions;

    @JsonProperty("floorValue")
    BigDecimal floorValue;

    @JsonProperty("floorRule")
    String floorRule;

    @JsonProperty("floorRuleValue")
    BigDecimal floorRuleValue;
}
