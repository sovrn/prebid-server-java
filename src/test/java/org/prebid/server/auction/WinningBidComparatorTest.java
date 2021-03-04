package org.prebid.server.auction;

import com.iab.openrtb.request.Deal;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Pmp;
import com.iab.openrtb.response.Bid;
import org.junit.Test;
import org.prebid.server.auction.model.BidInfo;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class WinningBidComparatorTest {

    private final WinningBidComparator target = new WinningBidComparator();

    @Test
    public void compareShouldReturnMoreThatZeroWhenFirstHasHigherPrice() {
        // given
        final BidInfo higherPriceBidInfo = givenBidInfo(5.0f);
        final BidInfo loverPriceBidInfo = givenBidInfo(1.0f);

        // when
        final int result = target.compare(higherPriceBidInfo, loverPriceBidInfo);

        // then
        assertThat(result).isGreaterThan(0);
    }

    @Test
    public void compareShouldReturnLessThatZeroWhenFirstHasLowerPrice() {
        // given
        final BidInfo loverPriceBidInfo = givenBidInfo(1.0f);
        final BidInfo higherPriceBidInfo = givenBidInfo(5.0f);

        // when
        final int result = target.compare(loverPriceBidInfo, higherPriceBidInfo);

        // then
        assertThat(result).isLessThan(0);
    }

    @Test
    public void compareShouldReturnZeroWhenPriceAreEqual() {
        // given
        final BidInfo bidInfo1 = givenBidInfo(5.0f);
        final BidInfo bidInfo2 = givenBidInfo(5.0f);

        // when
        final int result = target.compare(bidInfo1, bidInfo2);

        // then
        assertThat(result).isEqualTo(0);
    }

    @Test
    public void sortShouldReturnExpectedSortedResult() {
        // given
        final BidInfo bidInfo1 = givenBidInfo(1.0f);
        final BidInfo bidInfo2 = givenBidInfo(2.0f);
        final BidInfo bidInfo3 = givenBidInfo(4.1f);
        final BidInfo bidInfo4 = givenBidInfo(4.4f);
        final BidInfo bidInfo5 = givenBidInfo(5.0f);
        final BidInfo bidInfo6 = givenBidInfo(100.1f);

        final List<BidInfo> bidInfos = Arrays.asList(bidInfo5, bidInfo3, bidInfo1, bidInfo2, bidInfo1, bidInfo4,
                bidInfo6);

        // when
        bidInfos.sort(target);

        // then
        assertThat(bidInfos).containsOnly(bidInfo1, bidInfo1, bidInfo2, bidInfo3, bidInfo4, bidInfo5, bidInfo6);
    }

    @Test
    public void compareShouldReturnMoreThatZeroWhenFirstHasDeal() {
        // given
        final BidInfo dealPriceBidInfo = givenBidInfo(5.0f, "dealId", emptyList());
        final BidInfo higherPriceBidInfo = givenBidInfo(10.0f, null, emptyList());

        // when
        final int result = target.compare(dealPriceBidInfo, higherPriceBidInfo);

        // then
        assertThat(result).isGreaterThan(0);
    }

    @Test
    public void compareShouldReturnLessThatZeroWhenFirstHasNoDeal() {
        // given
        final BidInfo higherPriceBidInfo = givenBidInfo(10.0f, null, emptyList());
        final BidInfo dealPriceBidInfo = givenBidInfo(5.0f, "dealId", emptyList());

        // when
        final int result = target.compare(higherPriceBidInfo, dealPriceBidInfo);

        // then
        assertThat(result).isLessThan(0);
    }

    @Test
    public void compareShouldReturnZeroWhenBothHaveDealsAndImpHasNoDeals() {
        // given
        final BidInfo bidInfo1 = givenBidInfo(5.0f, "dealId", emptyList());
        final BidInfo bidInfo2 = givenBidInfo(5.0f, "dealId", emptyList());

        // when
        final int result = target.compare(bidInfo1, bidInfo2);

        // then
        assertThat(result).isEqualTo(0);
    }

    @Test
    public void compareShouldReturnZeroWhenBothHaveSameDealIdAndHasSamePrice() {
        // given
        final List<String> impDeals = singletonList("dealId");
        final BidInfo bidInfo1 = givenBidInfo(5.0f, "dealId", impDeals);
        final BidInfo bidInfo2 = givenBidInfo(5.0f, "dealId", impDeals);

        // when
        final int result = target.compare(bidInfo1, bidInfo2);

        // then
        assertThat(result).isEqualTo(0);
    }

    @Test
    public void compareShouldReturnMoreThanZeroWhenBothHaveSameDealIdAndFirstHasHigherPrice() {
        // given
        final List<String> impDeals = singletonList("dealId");
        final BidInfo bidInfo1 = givenBidInfo(10.0f, "dealId", impDeals);
        final BidInfo bidInfo2 = givenBidInfo(5.0f, "dealId", impDeals);

        // when
        final int result = target.compare(bidInfo1, bidInfo2);

        // then
        assertThat(result).isGreaterThan(0);
    }

    @Test
    public void compareShouldReturnLessThanZeroWhenFirstHasHigherPriceAndSecondHasLessImpDealIndex() {
        // given
        final List<String> impDeals = Arrays.asList("dealId1", "dealId2");
        final BidInfo bidInfo1 = givenBidInfo(10.0f, "dealId2", impDeals);
        final BidInfo bidInfo2 = givenBidInfo(5.0f, "dealId1", impDeals);

        // when
        final int result = target.compare(bidInfo1, bidInfo2);

        // then
        assertThat(result).isLessThan(0);
    }

    @Test
    public void sortShouldReturnExpectedSortedResultWithDeals() {
        // given
        final String dealId1 = "dealId1";
        final String dealId2 = "dealId2";
        final List<String> impDeals = Arrays.asList(dealId1, dealId2);

        final BidInfo bidInfo1 = givenBidInfo(1.0f, dealId1, impDeals);
        final BidInfo bidInfo2 = givenBidInfo(2.0f, dealId1, impDeals);
        final BidInfo bidInfo3 = givenBidInfo(4.1f, dealId2, impDeals);
        final BidInfo bidInfo4 = givenBidInfo(4.4f, "dealId3", impDeals);
        final BidInfo bidInfo5 = givenBidInfo(5.0f, null, impDeals);
        final BidInfo bidInfo6 = givenBidInfo(100.1f, null, impDeals);

        final List<BidInfo> bidInfos = Arrays.asList(bidInfo5, bidInfo3, bidInfo1, bidInfo2, bidInfo1, bidInfo4,
                bidInfo6);

        // when
        bidInfos.sort(target);

        // then
        assertThat(bidInfos).containsOnly(bidInfo5, bidInfo6, bidInfo4, bidInfo3, bidInfo2, bidInfo1, bidInfo1);
    }

    private static BidInfo givenBidInfo(float price) {
        return BidInfo.builder()
                .bid(Bid.builder().price(BigDecimal.valueOf(price)).build())
                .build();
    }

    private static BidInfo givenBidInfo(float price, String dealId, List<String> impDealIds) {
        final List<Deal> impDeals = impDealIds.stream()
                .map(impDealId -> Deal.builder().id(impDealId).build())
                .collect(Collectors.toList());
        final Pmp pmp = Pmp.builder().deals(impDeals).build();

        return BidInfo.builder()
                .bid(Bid.builder().price(BigDecimal.valueOf(price)).dealid(dealId).build())
                .correspondingImp(Imp.builder().pmp(pmp).build())
                .build();
    }
}
