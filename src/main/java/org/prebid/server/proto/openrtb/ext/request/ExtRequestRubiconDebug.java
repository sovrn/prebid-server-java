package org.prebid.server.proto.openrtb.ext.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.iab.openrtb.request.BidRequest;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.prebid.server.bidder.model.HttpCall;

/**
 * Defines the contract for bidrequest.ext.rubicon.debug
 */
@AllArgsConstructor(staticName = "of")
@Value
public class ExtRequestRubiconDebug {

    /**
     * This should be used only for testing.
     * <p>
     * CPM for all bids will be replaced with this field value
     * in {@link org.prebid.server.bidder.rubicon.RubiconBidder#makeBids(HttpCall, BidRequest)}.
     */
    @JsonProperty("cpmOverride")
    Float cpmOverride;
}
