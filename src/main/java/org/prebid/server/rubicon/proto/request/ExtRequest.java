package org.prebid.server.rubicon.proto.request;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class ExtRequest {

    ExtRequestPrebid prebid;
}
