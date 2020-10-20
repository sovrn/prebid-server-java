package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value(staticConstructor = "of")
public class Gdpr {

    @JsonProperty("pbsApplies")
    Boolean pbsApplies;

    Boolean applies;

    @JsonProperty("consentString")
    String consentString;

    Integer version;
}
