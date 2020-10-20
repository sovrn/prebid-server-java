package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Builder
@Value
public class AdUnit {

    @JsonProperty("transactionId")
    String transactionId;

    String status;

    Error error;

    @JsonProperty("mediaTypes")
    List<String> mediaTypes;

    @JsonProperty("videoAdFormat")
    String videoAdFormat;

    List<Dimensions> dimensions;

    @JsonProperty("adUnitCode")
    String adUnitCode;

    @JsonProperty("pbAdSlot")
    String pbAdSlot;

    Gam gam;

    @JsonProperty("adserverTargeting")
    Map<String, String> adserverTargeting;

    List<Bid> bids;

    @JsonProperty("siteId")
    Integer siteId;

    @JsonProperty("zoneId")
    Integer zoneId;
}
