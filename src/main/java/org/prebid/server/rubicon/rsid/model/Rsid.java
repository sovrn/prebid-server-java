package org.prebid.server.rubicon.rsid.model;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor(staticName = "of")
@Value
public class Rsid {

    String country;
}
