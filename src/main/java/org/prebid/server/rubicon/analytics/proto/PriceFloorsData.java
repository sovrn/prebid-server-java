package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;
import org.prebid.server.floors.model.PriceFloorLocation;
import org.prebid.server.floors.proto.FetchStatus;

import java.math.BigDecimal;

@Value
@Builder
public class PriceFloorsData {

    PriceFloorLocation location;

    @JsonProperty("fetchStatus")
    FetchStatus fetchStatus;

    Boolean skipped;

    @JsonProperty("modelName")
    String modelName;

    Boolean enforcement;

    @JsonProperty("dealsEnforced")
    Boolean dealsEnforced;

    @JsonProperty("skipRate")
    Integer skipRate;

    String provider;

    @JsonProperty("floorMin")
    BigDecimal floorMin;

    @JsonProperty("modelTimestamp")
    Long modelTimestamp;

    @JsonProperty("modelWeight")
    Integer modelWeight;
}
