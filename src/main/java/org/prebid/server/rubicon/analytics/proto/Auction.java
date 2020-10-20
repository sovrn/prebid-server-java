package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

import java.util.List;

@Value(staticConstructor = "of")
public class Auction {

    @JsonProperty("requestId")
    String requestId;

    @JsonProperty("samplingFactor")
    Integer samplingFactor;

    @JsonProperty("adUnits")
    List<AdUnit> adUnits;

    @JsonProperty("accountId")
    Integer accountId;

    @JsonProperty("serverTimeoutMillis")
    Long serverTimeoutMillis;

    @JsonProperty("hasRubiconId")
    Boolean hasRubiconId;

    Gdpr gdpr;
}
