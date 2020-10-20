package org.prebid.server.rubicon.analytics.proto;

import lombok.Value;

@Value(staticConstructor = "of")
public class User {

    Geo geo;
}
