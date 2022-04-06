package org.prebid.server.functional.tests.rubicon

import org.prebid.server.functional.model.ChannelType
import org.prebid.server.functional.model.config.AccountAnalyticsConfig
import org.prebid.server.functional.model.config.AccountConfig
import org.prebid.server.functional.model.db.Account
import org.prebid.server.functional.model.request.auction.Channel
import org.prebid.server.functional.model.response.auction.BidResponse
import org.prebid.server.functional.util.PBSUtils

import static org.prebid.server.functional.model.ChannelType.PBJS
import static org.prebid.server.functional.model.ChannelType.WEB
import static org.prebid.server.functional.model.request.auction.DistributionChannel.SITE

class RubiconAnalyticsSpec extends RubiconBaseSpec {

    def "PBS should send WEB to rubicon analytics adapter when ext.prebid.channel = #channelNameRequest, auctionEvents enabled for #channelNameAccount"() {
        given: "Rubicon BidRequest with channel: #channelNameRequest"
        def bidRequest = getRubiconBidRequest(SITE).tap {
            ext.prebid.channel = new Channel(name: channelNameRequest)
        }

        and: "Account with enabled auctionEvents for channel: #channelNameAccount"
        def account = getAccountWithEnabledAnalytics(bidRequest.site.publisher.id, channelNameAccount)
        accountDao.save(account)

        when: "PBS processes auction request"
        rubiconPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request channel should correspond to 'web'"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequest(bidRequest.id)
        assert analyticsRequest.channel == WEB

        where:
        channelNameRequest | channelNameAccount
        WEB                | WEB
        WEB                | PBJS
        PBJS               | WEB
        PBJS               | PBJS
    }

    def "PBS should not validate dealid when it contains #description"() {
        given: "Rubicon BidRequest"
        def bidRequest = getRubiconBidRequest(SITE)

        and: "Account with enabled analytics"
        def account = getAccountWithEnabledAnalytics(bidRequest.site.publisher.id)
        accountDao.save(account)

        and: "Bid response with dealid"
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().dealid = dealid
        }
        bidder.setResponse(bidRequest.id, bidResponse)

        when: "PBS processes auction request"
        rubiconPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request dealId should correspond to dealId from bidder response"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequest(bidRequest.id)
        assert analyticsRequest.auctions.first().adUnits.first().bids.first().bidResponse?.dealId == dealid as String

        where:
        description | dealid
        "letters"   | PBSUtils.randomString
        "numbers"   | PBSUtils.randomNumber
    }

    private static Account getAccountWithEnabledAnalytics(String accountId, ChannelType channelType = WEB) {
        def analytics = new AccountAnalyticsConfig(auctionEvents: [(channelType): true])
        def accountConfig = new AccountConfig(analytics: analytics)
        new Account(uuid: accountId, config: accountConfig)
    }
}
