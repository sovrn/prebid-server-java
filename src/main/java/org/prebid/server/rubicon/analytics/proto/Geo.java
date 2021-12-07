package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value(staticConstructor = "of")
public class Geo {

    /**
     * Country in ISO-3166-1-alpha-2 format.
     */
    String country;

    @JsonProperty("metroCode")
    Integer metroCode;
}
