package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class App {

    public static final App EMPTY = App.of(null, null, null, null);

    String bundle;

    String version;

    @JsonProperty("sdkVersion")
    String sdkVersion;

    @JsonProperty("sdkSource")
    String sdkSource;
}
