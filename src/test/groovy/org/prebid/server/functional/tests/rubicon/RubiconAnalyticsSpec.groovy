package org.prebid.server.functional.tests.rubicon

import org.prebid.server.functional.model.config.AccountAnalyticsConfig
import org.prebid.server.functional.model.config.AccountConfig
import org.prebid.server.functional.model.db.Account
import org.prebid.server.functional.model.request.auction.Channel

import static org.prebid.server.functional.model.ChannelType.PBJS
import static org.prebid.server.functional.model.ChannelType.WEB
import static org.prebid.server.functional.model.request.auction.DistributionChannel.SITE

class RubiconAnalyticsSpec extends RubiconBaseSpec {

    def "PBS should send WEB to rubicon analytics adapter when ext.prebid.channel = #channelNameRequest, auctionEvents enabled for #channelNameDb"() {
        given: "Rubicon BidRequest with channel: #channelNameRequest"
        def bidRequest = getRubiconBidRequest(SITE).tap {
            ext.prebid.channel = new Channel(name: channelNameRequest)
        }

        and: "Account with enabled auctionEvents for channel: #channelNameDb"
        def analytics = new AccountAnalyticsConfig(auctionEvents: [(channelNameDb): true])
        def accountConfig = new AccountConfig(analytics: analytics)
        def account = new Account(uuid: bidRequest.site.publisher.id, config: accountConfig)
        accountDao.save(account)

        when: "PBS processes auction request"
        rubiconPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request channel should correspond to 'web'"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequest(bidRequest.id)
        assert analyticsRequest.channel == WEB

        where:
        channelNameRequest | channelNameDb
        WEB                | WEB
        WEB                | PBJS
        PBJS               | WEB
        PBJS               | PBJS
    }
}
