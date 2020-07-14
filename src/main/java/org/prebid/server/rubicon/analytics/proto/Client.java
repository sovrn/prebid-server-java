package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class Client {

    public static final Client EMPTY = Client.builder().build();

    @JsonProperty("deviceClass")
    String deviceClass;

    String os;

    @JsonProperty("osVersion")
    String osVersion;

    String make;

    String model;

    String carrier;

    @JsonProperty("connectionType")
    Integer connectionType;

    App app;
}
