package org.prebid.server.auction;

import com.iab.openrtb.request.Deal;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Pmp;
import com.iab.openrtb.response.Bid;
import org.junit.Test;
import org.prebid.server.auction.model.BidderResponse;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderSeatBid;
import org.prebid.server.proto.openrtb.ext.response.BidType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class BidResponseReducerTest {

    private final BidResponseReducer bidResponseReducer = new BidResponseReducer();

    @Test
    public void removeRedundantBidsShouldReduceNonDealBidsByPriceDroppingNonDealsBids() {
        // given
        final BidderResponse bidderResponse = BidderResponse.of(
                "bidder1",
                givenSeatBid(
                        givenBidderBid("bidId1", "impId1", "dealId1", 5.0f), // deal
                        givenBidderBid("bidId2", "impId1", "dealId2", 6.0f), // deal
                        givenBidderBid("bidId3", "impId1", null, 7.0f) // non deal
                ),
                0);

        // when
        final BidderResponse resultBidderResponse = bidResponseReducer.removeRedundantBids(bidderResponse,
                singletonList(givenImp("impId1")));

        // then
        assertThat(resultBidderResponse.getSeatBid().getBids())
                .extracting(BidderBid::getBid)
                .extracting(Bid::getId)
                .containsOnly("bidId2");
    }

    @Test
    public void removeRedundantBidsShouldReduceNonDealBidsByPrice() {
        // given
        final BidderResponse bidderResponse = BidderResponse.of(
                "bidder1",
                givenSeatBid(
                        givenBidderBid("bidId1", "impId1", null, 5.0f), // non deal
                        givenBidderBid("bidId2", "impId1", null, 6.0f), // non deal
                        givenBidderBid("bidId3", "impId1", null, 7.0f) // non deal
                ),
                0);

        // when
        final BidderResponse resultBidderResponse = bidResponseReducer.removeRedundantBids(bidderResponse,
                singletonList(givenImp("impId1")));

        // then
        assertThat(resultBidderResponse.getSeatBid().getBids())
                .extracting(BidderBid::getBid)
                .extracting(Bid::getId)
                .containsOnly("bidId3");
    }

    @Test
    public void removeRedundantBidsShouldNotReduceBids() {
        // given
        final BidderResponse bidderResponse = BidderResponse.of(
                "bidder1",
                givenSeatBid(givenBidderBid("bidId1", "impId1", null, 5.0f)),
                0);

        // when
        final BidderResponse resultBidderResponse = bidResponseReducer.removeRedundantBids(bidderResponse,
                singletonList(givenImp("impId1")));

        // then
        assertThat(resultBidderResponse.getSeatBid().getBids())
                .extracting(BidderBid::getBid)
                .extracting(Bid::getId)
                .containsOnly("bidId1");
    }

    @Test
    public void removeRedundantBidsShouldReduceAllTypesOfBidsForMultipleImps() {
        // given
        final BidderResponse bidderResponse = BidderResponse.of(
                "bidder1",
                givenSeatBid(
                        givenBidderBid("bidId1-1", "impId1", "dealId1", 6.0f), // PG deal
                        givenBidderBid("bidId2-1", "impId1", "dealId2", 5.0f), // PG deal
                        givenBidderBid("bidId3-1", "impId1", "dealId3", 5.0f), // deal
                        givenBidderBid("bidId4-1", "impId1", null, 5.0f), // non deal
                        givenBidderBid("bidId1-2", "impId2", "dealId4", 5.0f), // deal
                        givenBidderBid("bidId2-2", "impId2", "dealId5", 6.0f), // deal
                        givenBidderBid("bidId3-2", "impId2", null, 5.0f), // non deal
                        givenBidderBid("bidId1-3", "impId3", null, 5.0f), // non deal
                        givenBidderBid("bidId2-3", "impId3", null, 6.0f)  // non deal
                ),
                0);

        // when
        final BidderResponse resultBidderResponse = bidResponseReducer.removeRedundantBids(bidderResponse,
                asList(
                        givenImp("impId1", "dealId2", "dealId1"),
                        givenImp("impId2"),
                        givenImp("impId3")));

        // then
        assertThat(resultBidderResponse.getSeatBid().getBids())
                .extracting(BidderBid::getBid)
                .extracting(Bid::getId)
                .containsOnly("bidId2-1", "bidId2-2", "bidId2-3");
    }

    @Test
    public void removeRedundantBidsShouldReducePgDealsBidsByTopDealDroppingNonPgBids() {
        // given
        final BidderResponse bidderResponse = BidderResponse.of("bidder1",
                givenSeatBid(
                        givenBidderBid("bidId1", "impId1", "dealId1", 6.0f), // PG deal
                        givenBidderBid("bidId2", "impId1", "dealId2", 5.0f), // PG deal
                        givenBidderBid("bidId3", "impId1", "dealId3", 5.0f), // deal
                        givenBidderBid("bidId4", "impId1", null, 5.0f) // non deal
                ),
                0);

        // when
        final BidderResponse resultBidderResponse = bidResponseReducer.removeRedundantBids(bidderResponse,
                singletonList(givenImp("impId1", "dealId2", "dealId1")));

        // then
        assertThat(resultBidderResponse.getSeatBid().getBids())
                .extracting(BidderBid::getBid)
                .extracting(Bid::getId)
                .containsOnly("bidId2");
    }

    @Test
    public void removeRedundantBidsShouldReducePgDealsWhenTopDealIsNotPresentInBids() {
        // given
        final BidderResponse bidderResponse = BidderResponse.of("bidder1",
                givenSeatBid(
                        givenBidderBid("bidId1", "impId1", "dealId1", 6.0f), // PG deal
                        givenBidderBid("bidId2", "impId1", "dealId2", 5.0f), // PG deal
                        givenBidderBid("bidId3", "impId1", "dealId3", 5.0f), // deal
                        givenBidderBid("bidId4", "impId1", null, 5.0f) // non deal
                ),
                0);

        // when
        final BidderResponse resultBidderResponse = bidResponseReducer.removeRedundantBids(bidderResponse,
                singletonList(givenImp("impId1", "dealTop", "dealId2", "dealId1")));

        // then
        assertThat(resultBidderResponse.getSeatBid().getBids())
                .extracting(BidderBid::getBid)
                .extracting(Bid::getId)
                .containsOnly("bidId2");
    }

    private static BidderBid givenBidderBid(String bidId, String impId, String dealId, float price) {
        return BidderBid.of(
                Bid.builder().id(bidId).impid(impId).dealid(dealId).price(BigDecimal.valueOf(price)).build(),
                BidType.banner, "USD");
    }

    private static BidderSeatBid givenSeatBid(BidderBid... bidderBids) {
        return BidderSeatBid.of(asList(bidderBids), null, null);
    }

    private static Imp givenImp(String impId, String... dealIds) {
        return Imp.builder().id(impId).pmp(Pmp.builder().deals(
                dealIds != null
                        ? Arrays.stream(dealIds)
                        .map(dealId -> Deal.builder().id(dealId).build())
                        .collect(Collectors.toList())
                        : null).build()).build();
    }
}
