package org.prebid.server.rubicon.analytics.proto;

import lombok.Value;

@Value(staticConstructor = "of")
public class Geo {

    String country;

    Integer dma;
}
