package org.prebid.server.rubicon.proto.request;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class ExtRequestPrebidBiddersRubicon {

    public static final ExtRequestPrebidBiddersRubicon EMPTY = ExtRequestPrebidBiddersRubicon.of(null, null);

    String integration;

    String wrappername;
}
