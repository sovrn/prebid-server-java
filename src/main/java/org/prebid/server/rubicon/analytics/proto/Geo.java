package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value(staticConstructor = "of")
public class Geo {

    String country;

    @JsonProperty("metroCode")
    Integer metroCode;
}
