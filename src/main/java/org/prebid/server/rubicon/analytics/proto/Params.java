package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class Params {

    private static final Params EMPTY_PARAM = Params.of(null, null, null);

    @JsonProperty("accountId")
    Integer accountId;

    @JsonProperty("siteId")
    Integer siteId;

    @JsonProperty("zoneId")
    Integer zoneId;

    public static Params empty() {
        return EMPTY_PARAM;
    }
}
