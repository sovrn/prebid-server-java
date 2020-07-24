package org.prebid.server.validation;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Deal;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Pmp;
import com.iab.openrtb.response.Bid;
import org.junit.Before;
import org.junit.Test;
import org.prebid.server.VertxTest;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.proto.openrtb.ext.request.ExtDeal;
import org.prebid.server.proto.openrtb.ext.request.ExtDealLine;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.validation.model.ValidationResult;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;

public class ResponseBidValidatorTest extends VertxTest {

    private ResponseBidValidator responseBidValidator;

    @Before
    public void setUp() {
        responseBidValidator = new ResponseBidValidator(jacksonMapper, true);
    }

    @Test
    public void validateShouldFailIfMissingBid() {
        final ValidationResult result = responseBidValidator.validate(BidderBid.of(null, null, null),
                givenRequest(identity()));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Empty bid object submitted.");
    }

    @Test
    public void validateShouldFailIfBidHasNoId() {
        final ValidationResult result = responseBidValidator.validate(givenBid(builder -> builder.id(null)),
                givenRequest(identity()));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid missing required field 'id'");
    }

    @Test
    public void validateShouldFailIfBidHasNoImpId() {
        final ValidationResult result = responseBidValidator.validate(givenBid(builder -> builder.impid(null)),
                givenRequest(identity()));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" missing required field 'impid'");
    }

    @Test
    public void validateShouldFailIfBidHasNoPrice() {
        final ValidationResult result = responseBidValidator.validate(givenBid(builder -> builder.price(null)),
                givenRequest(identity()));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" does not contain a positive 'price'");
    }

    @Test
    public void validateShouldFailIfBidHasNegativePrice() {
        final ValidationResult result = responseBidValidator.validate(givenBid(builder -> builder.price(
                BigDecimal.valueOf(-1))), givenRequest(identity()));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" does not contain a positive 'price'");
    }

    @Test
    public void validateShouldFailIfBidHasNoCrid() {
        final ValidationResult result = responseBidValidator.validate(givenBid(builder -> builder.crid(null)),
                givenRequest(identity()));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" missing creative ID");
    }

    @Test
    public void validateShouldFailIfBidHasNoCorrespondingImp() {
        final ValidationResult result = responseBidValidator.validate(givenBid(identity()),
                givenRequest(imp -> imp.id("non-existing")));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" has no corresponding imp in request");
    }

    @Test
    public void validateShouldReturnSuccessfulResultForValidNonDealBid() {
        final ValidationResult result = responseBidValidator.validate(givenBid(identity()),
                givenRequest(imp -> imp.ext(mapper.createObjectNode().putNull("bidder"))));

        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    public void validateShouldFailIfBidHasNoDealid() {
        final ValidationResult result = responseBidValidator.validate(givenBid(identity()), givenRequest(identity()));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" missing required field 'dealid'");
    }

    @Test
    public void validateShouldFailIfBidHasDealidAndImpHasNoDeals() {
        final ValidationResult result = responseBidValidator.validate(givenBid(bid -> bid.dealid("dealId1")),
                givenRequest(identity()));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" has 'dealid' not present in corresponding imp in request. 'dealid' in "
                        + "bid: 'dealId1', deal Ids in imp: ''");
    }

    @Test
    public void validateShouldFailIfBidHasDealidMissingInImp() {
        final ValidationResult result = responseBidValidator.validate(givenBid(bid -> bid.dealid("dealId1")),
                givenRequest(imp -> imp.pmp(pmp(asList(deal(d -> d.id("dealId2")), deal(d -> d.id("dealId3")))))));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" has 'dealid' not present in corresponding imp in request. 'dealid' in "
                        + "bid: 'dealId1', deal Ids in imp: 'dealId2,dealId3'");
    }

    @Test
    public void validateShouldFailIfBidIsBannerAndImpHasNoBanner() {
        final ValidationResult result = responseBidValidator.validate(
                givenBid(bid -> bid.dealid("dealId1"), BidType.banner),
                givenRequest(imp -> imp.pmp(pmp(singletonList(deal(d -> d.id("dealId1")))))));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" has banner media type but corresponding imp in request is missing "
                        + "'banner' object");
    }

    @Test
    public void validateShouldFailIfBidIsBannerAndSizeHasNoMatchInBannerFormats() {
        final ValidationResult result = responseBidValidator.validate(
                givenBid(bid -> bid.dealid("dealId1").w(300).h(400), BidType.banner),
                givenRequest(imp -> imp.pmp(pmp(singletonList(deal(d -> d.id("dealId1")))))
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(500).build()))
                                .build())));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" has 'w' and 'h' not supported by corresponding imp in request. Bid "
                        + "dimensions: '300x400', formats in imp: '400x500'");
    }

    @Test
    public void validateShouldFailIfBidIsBannerAndSizeHasNoMatchInLineItem() {
        final ValidationResult result = responseBidValidator.validate(
                givenBid(bid -> bid.dealid("dealId1").w(300).h(400), BidType.banner),
                givenRequest(imp -> imp
                        .pmp(pmp(singletonList(deal(d -> d
                                .id("dealId1")
                                .ext(mapper.valueToTree(ExtDeal.of(ExtDealLine.of(null, null,
                                        singletonList(Format.builder().w(500).h(600).build()), null))))))))
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(300).h(400).build()))
                                .build())));

        assertThat(result.getErrors()).hasSize(1)
                .containsOnly("Bid \"bidId1\" has 'w' and 'h' not matched to Line Item. Bid dimensions: '300x400', "
                        + "Line Item sizes: '500x600'");
    }

    @Test
    public void validateShouldReturnSuccessfulResultForValidDealNonBannerBid() {
        final ValidationResult result = responseBidValidator.validate(givenBid(bid -> bid.dealid("dealId1")),
                givenRequest(imp -> imp.pmp(pmp(singletonList(deal(d -> d.id("dealId1")))))));

        assertThat(result.hasErrors()).isFalse();
    }

    @Test
    public void validateShouldReturnSuccessfulResultForValidDealBannerBid() {
        final ValidationResult result = responseBidValidator.validate(
                givenBid(bid -> bid.dealid("dealId1").w(300).h(400), BidType.banner),
                givenRequest(imp -> imp
                        .pmp(pmp(singletonList(deal(d -> d
                                .id("dealId1")
                                .ext(mapper.valueToTree(ExtDeal.of(ExtDealLine.of(null, null,
                                        singletonList(Format.builder().w(300).h(400).build()), null))))))))
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(300).h(400).build()))
                                .build())));

        assertThat(result.hasErrors()).isFalse();
    }

    private Deal deal(Function<Deal.DealBuilder, Deal.DealBuilder> dealCustomizer) {
        final Deal.DealBuilder builder = Deal.builder();
        return dealCustomizer.apply(builder).build();
    }

    private BidRequest givenRequest(Function<Imp.ImpBuilder, Imp.ImpBuilder> impCustomizer) {
        final ObjectNode ext = mapper.createObjectNode();
        ext.putObject("bidder").put("dealsonly", true);

        final Imp.ImpBuilder impBuilder = Imp.builder()
                .id("impId1")
                .ext(ext);
        final Imp imp = impCustomizer.apply(impBuilder).build();

        return BidRequest.builder().imp(singletonList(imp)).build();
    }

    private static BidderBid givenBid(Function<Bid.BidBuilder, Bid.BidBuilder> bidCustomizer, BidType mediaType) {
        final Bid.BidBuilder bidBuilder = Bid.builder()
                .id("bidId1")
                .impid("impId1")
                .crid("crid1")
                .price(BigDecimal.ONE);
        return BidderBid.of(bidCustomizer.apply(bidBuilder).build(), mediaType, null);
    }

    private static BidderBid givenBid(Function<Bid.BidBuilder, Bid.BidBuilder> bidCustomizer) {
        return givenBid(bidCustomizer, null);
    }

    private static Pmp pmp(List<Deal> deals) {
        return Pmp.builder().deals(deals).build();
    }
}
