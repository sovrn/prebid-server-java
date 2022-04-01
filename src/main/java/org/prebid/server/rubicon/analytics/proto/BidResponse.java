package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.math.BigDecimal;

@AllArgsConstructor(staticName = "of")
@Value
public class BidResponse {

    @JsonProperty("dealId")
    String dealId;

    @JsonProperty("bidPriceUSD")
    BigDecimal bidPriceUsd;

    @JsonProperty("mediaType")
    String mediaType;

    Dimensions dimensions;
}
