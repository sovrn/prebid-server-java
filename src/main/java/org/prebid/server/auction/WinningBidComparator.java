package org.prebid.server.auction;

import com.iab.openrtb.request.Deal;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Pmp;
import com.iab.openrtb.response.Bid;
import org.apache.commons.collections4.CollectionUtils;
import org.prebid.server.auction.model.BidInfo;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Compares two {@link BidInfo} arguments for order.
 * <p>
 * Returns a negative integer when first is less valuable than second
 * Zero when arguments are equal by their winning value
 * Positive integer when first have more value then second
 *
 * <p>
 * The priority for choosing the 'winner' (hb_pb, hb_bidder, etc) is:
 * <p>
 * - PG Line Items always win over non-PG bids
 * - Amongst PG Line Items, choose the highest CPM
 * - Amongst non-PG bids, choose the highest CPM
 */
public class WinningBidComparator implements Comparator<BidInfo> {

    private final Comparator<BidInfo> priceComparator = Comparator.comparing(o -> o.getBid().getPrice());
    private final Comparator<BidInfo> dealComparator = new DealComparator();
    private final Comparator<BidInfo> winningBidComparator = dealComparator.thenComparing(priceComparator);

    @Override
    public int compare(BidInfo bidInfo1, BidInfo bidInfo2) {
        final Imp imp = bidInfo1.getCorrespondingImp();
        // this should never happen
        if (!Objects.equals(imp, bidInfo2.getCorrespondingImp())) {
            throw new IllegalStateException(
                    String.format("Error while determining winning bid: "
                            + "Multiple bids was found for impId: %s", imp.getId()));
        }

        return winningBidComparator.compare(bidInfo1, bidInfo2);
    }

    private static class DealComparator implements Comparator<BidInfo> {

        private final Comparator<Integer> dealIndexComparator = Comparator.comparingInt(Integer::intValue).reversed();

        @Override
        public int compare(BidInfo bidInfo1, BidInfo bidInfo2) {
            final Bid bid1 = bidInfo1.getBid();
            final Bid bid2 = bidInfo2.getBid();
            final String bidDealId1 = bid1.getDealid();
            final String bidDealId2 = bid2.getDealid();

            final boolean isPresentBidDealId1 = bidDealId1 != null;
            final boolean isPresentBidDealId2 = bidDealId2 != null;

            if (!isPresentBidDealId1 && !isPresentBidDealId2) {
                return 0;
            }

            if (isPresentBidDealId1 && isPresentBidDealId2) {
                return compareDealBidInfos(bidInfo1, bidInfo2);
            }

            return isPresentBidDealId1 ? 1 : -1;
        }

        private int compareDealBidInfos(BidInfo bidInfo1, BidInfo bidInfo2) {
            final Imp imp = bidInfo1.getCorrespondingImp();
            final Pmp pmp = imp.getPmp();
            final List<Deal> impDeals = pmp != null ? pmp.getDeals() : null;

            if (CollectionUtils.isEmpty(impDeals)) {
                return 0;
            }

            final Bid bid1 = bidInfo1.getBid();
            final Bid bid2 = bidInfo2.getBid();

            int indexOfBidDealId1 = -1;
            int indexOfBidDealId2 = -1;

            // search for indexes of deals
            for (int i = 0; i < impDeals.size(); i++) {
                final String dealId = impDeals.get(i).getId();
                if (Objects.equals(dealId, bid1.getDealid())) {
                    indexOfBidDealId1 = i;
                }
                if (Objects.equals(dealId, bid2.getDealid())) {
                    indexOfBidDealId2 = i;
                }
            }

            final boolean isPresentImpDealId1 = indexOfBidDealId1 != -1;
            final boolean isPresentImpDealId2 = indexOfBidDealId2 != -1;

            final boolean isOneOrBothDealIdNotPresent = !isPresentImpDealId1 || !isPresentImpDealId2;
            return isOneOrBothDealIdNotPresent
                    ? isPresentImpDealId1 ? 1 : -1 // case when no deal IDs found is covered by response validator
                    : dealIndexComparator.compare(indexOfBidDealId1, indexOfBidDealId2);
        }
    }
}
