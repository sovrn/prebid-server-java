package org.prebid.server.functional.tests

import org.prebid.server.functional.model.request.auction.BidRequest
import org.prebid.server.functional.model.response.auction.BidResponse
import org.prebid.server.functional.service.PrebidServerService

import static org.prebid.server.functional.model.Currency.EUR
import static org.prebid.server.functional.model.Currency.JPY
import static org.prebid.server.functional.model.Currency.USD
import static org.prebid.server.functional.testcontainers.PbsConfig.getExternalCurrencyConverterConfig

class CurrencySpec extends BaseSpec {

    private static final PrebidServerService pbsService = pbsServiceFactory.getService(externalCurrencyConverterConfig)

    def "PBS should use default server currency if not specified in the request"() {
        given: "Default BidRequest without currency"
        def bidRequest = BidRequest.defaultBidRequest.tap { cur = null }

        when: "PBS processes auction request"
        def bidResponse = pbsService.sendAuctionRequest(bidRequest)

        then: "Auction response should contain default currency"
        assert bidResponse.cur == DEFAULT_CURRENCY

        and: "Bidder request should contain default currency"
        def bidderRequest = bidder.getBidderRequest(bidRequest.id)
        assert bidderRequest.cur == [DEFAULT_CURRENCY]
    }

    def "PBS should treat bids without currency as in default server currency"() {
        given: "Default BidRequest without currency"
        def bidRequest = BidRequest.defaultBidRequest.tap { cur = null }

        and: "Bid without currency"
        def bidderResponse = BidResponse.getDefaultBidResponse(bidRequest).tap { cur = null }

        when: "PBS processes auction request"
        bidder.setResponse(bidRequest.id, bidderResponse)
        def bidResponse = pbsService.sendAuctionRequest(bidRequest)

        then: "Auction response should contain default currency"
        assert bidResponse.cur == DEFAULT_CURRENCY
        assert bidResponse.seatbid[0].bid[0].price == bidderResponse.seatbid[0].bid[0].price
    }

    def "PBS should convert #bidCurrency bid currency to #requestCurrency BidRequest currency"() {
        given: "Default BidRequest with #requestCurrency currency"
        def bidRequest = BidRequest.defaultBidRequest.tap { cur = [requestCurrency] }

        and: "Default Bid with a #bidCurrency currency"
        def bidderResponse = BidResponse.getDefaultBidResponse(bidRequest).tap { cur = bidCurrency }

        when: "PBS processes auction request"
        bidder.setResponse(bidRequest.id, bidderResponse)
        def bidResponse = pbsService.sendAuctionRequest(bidRequest)

        then: "Auction response should contain bid in #requestCurrency currency"
        assert bidResponse.cur == requestCurrency
        def bidPrice = bidResponse.seatbid[0].bid[0].price
        assert bidPrice == convertCurrency(bidderResponse.seatbid[0].bid[0].price, bidCurrency, requestCurrency)
        assert bidResponse.seatbid[0].bid[0].ext.origbidcpm == bidderResponse.seatbid[0].bid[0].price
        assert bidResponse.seatbid[0].bid[0].ext.origbidcur == bidCurrency

        where:
        requestCurrency || bidCurrency
        USD             || EUR
        EUR             || USD
    }

    def "PBS should use reverse currency conversion when direct conversion is not available"() {
        given: "Default BidRequest with #requestCurrency currency"
        def bidRequest = BidRequest.defaultBidRequest.tap { cur = [requestCurrency] }

        and: "Default Bid with a #bidCurrency currency"
        def bidderResponse = BidResponse.getDefaultBidResponse(bidRequest).tap { cur = bidCurrency }

        when: "PBS processes auction request"
        bidder.setResponse(bidRequest.id, bidderResponse)
        def bidResponse = pbsService.sendAuctionRequest(bidRequest)

        then: "Auction response should contain bid in #requestCurrency currency"
        assert bidResponse.cur == requestCurrency
        def bidPrice = bidResponse.seatbid[0].bid[0].price
        assert bidPrice == convertCurrency(bidderResponse.seatbid[0].bid[0].price, bidCurrency, requestCurrency)
        assert bidResponse.seatbid[0].bid[0].ext.origbidcpm == bidderResponse.seatbid[0].bid[0].price
        assert bidResponse.seatbid[0].bid[0].ext.origbidcur == bidCurrency

        where:
        requestCurrency || bidCurrency
        USD             || JPY
        JPY             || USD
    }
}
