package org.prebid.server.rubicon.analytics.proto;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class Dimensions {

    Integer width;

    Integer height;
}
