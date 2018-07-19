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
     * Added for case Rubicon Exchange responds with a test bid with zero CPM.
     * This field value will replace zero CPM for all bids
     * in {@link org.prebid.server.bidder.rubicon.RubiconBidder#makeBids(HttpCall, BidRequest)}.
     */
    @JsonProperty("cpmOverride")
    Float cpmOverride;
}
