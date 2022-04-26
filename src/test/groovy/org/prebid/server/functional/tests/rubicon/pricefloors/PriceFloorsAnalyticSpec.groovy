package org.prebid.server.functional.tests.rubicon.pricefloors

import org.prebid.server.functional.model.bidder.Rubicon
import org.prebid.server.functional.model.db.StoredRequest
import org.prebid.server.functional.model.mock.services.rubiconanalytics.Status
import org.prebid.server.functional.model.pricefloors.MediaType
import org.prebid.server.functional.model.pricefloors.PriceFloorData
import org.prebid.server.functional.model.pricefloors.PriceFloorSchema
import org.prebid.server.functional.model.pricefloors.Rule
import org.prebid.server.functional.model.request.amp.AmpRequest
import org.prebid.server.functional.model.request.auction.BidAdjustmentFactors
import org.prebid.server.functional.model.request.auction.BidRequest
import org.prebid.server.functional.model.request.auction.DistributionChannel
import org.prebid.server.functional.model.request.auction.ExtPrebidFloors
import org.prebid.server.functional.model.request.auction.ExtPrebidPriceFloorEnforcement
import org.prebid.server.functional.model.request.auction.PrebidStoredRequest
import org.prebid.server.functional.model.request.auction.Targeting
import org.prebid.server.functional.model.response.auction.Bid
import org.prebid.server.functional.model.response.auction.BidResponse
import org.prebid.server.functional.service.PrebidServerService
import org.prebid.server.functional.tests.pricefloors.PriceFloorsBaseSpec
import org.prebid.server.functional.tests.rubicon.RubiconBaseSpec
import org.prebid.server.functional.util.PBSUtils
import spock.lang.Shared

import static org.prebid.server.functional.model.Currency.EUR
import static org.prebid.server.functional.model.Currency.USD
import static org.prebid.server.functional.model.bidder.BidderName.RUBICON
import static org.prebid.server.functional.model.mock.services.rubiconanalytics.Status.REJECTED_IPF
import static org.prebid.server.functional.model.pricefloors.MediaType.BANNER
import static org.prebid.server.functional.model.pricefloors.MediaType.VIDEO
import static org.prebid.server.functional.model.pricefloors.PriceFloorField.MEDIA_TYPE
import static org.prebid.server.functional.model.request.auction.DistributionChannel.APP
import static org.prebid.server.functional.model.request.auction.FetchStatus.NONE
import static org.prebid.server.functional.model.request.auction.FetchStatus.SUCCESS
import static org.prebid.server.functional.model.request.auction.Location.FETCH
import static org.prebid.server.functional.model.request.auction.Location.NO_DATA
import static org.prebid.server.functional.model.request.auction.Location.REQUEST
import static org.prebid.server.functional.testcontainers.PbsConfig.getExternalCurrencyConverterConfig
import static org.prebid.server.functional.tests.pricefloors.PriceFloorsBaseSpec.FLOOR_MIN
import static org.prebid.server.functional.tests.pricefloors.PriceFloorsBaseSpec.floorsConfig
import static org.prebid.server.functional.tests.pricefloors.PriceFloorsBaseSpec.rule

class PriceFloorsAnalyticSpec extends RubiconBaseSpec {

    @Shared
    final PrebidServerService floorsPbsService = pbsServiceFactory.getService(
            RUBICON_CONFIG + ANALYTICS_CONFIG + floorsConfig + externalCurrencyConverterConfig)

    def "PBS should pass 'location: noData' to analytics adapters when neither fetched floors nor ext.prebid.floors exist"() {
        given: "Rubicon BidRequest"
        def bidRequest = rubiconBidRequest

        and: "Account with disabled fetch in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id).tap {
            config.auction.priceFloors.fetch.enabled = false
        }
        accountDao.save(account)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data, location = noData"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequest(bidRequest.id)
        verifyAll(analyticsRequest) {
            !auctions?.first()?.adUnits[0].bids[0].bidResponse.floorValue
            !auctions?.first()?.adUnits[0].bids[0].bidResponse.floorRule
            !auctions?.first()?.adUnits[0].bids[0].bidResponse.floorRuleValue

            auctions?.first()?.floors?.location == NO_DATA
            auctions?.first()?.floors?.fetchStatus == NONE
            !auctions?.first()?.floors?.skipped
            !auctions?.first()?.floors?.modelName
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            !auctions?.first()?.floors?.skipRate
            !auctions?.first()?.floors?.provider
            !auctions?.first()?.floors?.floorMin
            !auctions?.first()?.floors?.modelTimestamp
            !auctions?.first()?.floors?.modelWeight
        }
    }

    def "PBS should pass information about rejection when PF enforcement was applied for auction request"() {
        given: "Rubicon BidRequest with rubicon bidder, rubicon alias bidder"
        def alias = "rubiconAlias"
        def bidderParam = PBSUtils.randomNumber
        def bidderAliasParam = PBSUtils.randomNumber
        def bidRequest = rubiconBidRequest.tap {
            ext.prebid.aliases = [(alias): RUBICON]
            imp[0].ext.prebid.bidder.rubiconAlias = Rubicon.default.tap { accountId = bidderParam }
            imp[0].ext.prebid.bidder.rubicon.accountId = bidderAliasParam
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorValue]
        }
        floorsProvider.setResponse(bidRequest.app.publisher.id, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        and: "Bid response for rubicon bidder"
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().price = floorValue
        }
        bidder.setResponse(bidder.getRequest(bidderParam as String, "app.publisher.ext.rp.account_id"), bidResponse)

        and: "Bid response for rubicon bidder alias"
        def losingBidPrice = floorValue - 0.1
        def aliasBidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().price = losingBidPrice
        }
        bidder.setResponse(bidder.getRequest(bidderAliasParam as String, "app.publisher.ext.rp.account_id"), aliasBidResponse)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data from floors provider, information about rejected bid"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(bidRequest.id).last()
        def winningBid = analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.findAll { it.status == Status.SUCCESS }
        def losingBid = analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.findAll { it.status == REJECTED_IPF }
        verifyAll(analyticsRequest) {
            winningBid?.size() == 1
            losingBid?.size() == 1
            winningBid.first().bidResponse?.bidPriceUsd == floorValue as Float
            !losingBid.first().bidResponse?.bidPriceUsd
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorValue } ==
                    [floorValue, floorValue]
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorRule } ==
                    [rule, rule]
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorRuleValue } ==
                    [floorValue, floorValue]

            auctions?.first()?.floors?.location == FETCH
            auctions?.first()?.floors?.fetchStatus == SUCCESS
            !auctions?.first()?.floors?.skipped
            !auctions?.first()?.floors?.modelName
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            auctions?.first()?.floors?.skipRate == floorsResponse.skipRate
            auctions?.first()?.floors?.provider == floorsResponse.floorProvider
            !auctions?.first()?.floors?.floorMin
            !auctions?.first()?.floors?.modelTimestamp
            !auctions?.first()?.floors?.modelWeight
        }
    }

    def "PBS should pass information about rejection when PF enforcement was applied for amp request"() {
        given: "Default AmpRequest"
        def ampRequest = AmpRequest.defaultAmpRequest

        and: "Default stored request with floors "
        def alias = "rubiconAlias"
        def bidderParam = PBSUtils.randomNumber
        def bidderAliasParam = PBSUtils.randomNumber
        def ampStoredRequest = rubiconStoredRequestWithFloors.tap {
            ext.prebid.aliases = [(alias): RUBICON]
            imp[0].ext.prebid.bidder.rubiconAlias = Rubicon.default.tap { accountId = bidderParam }
            imp[0].ext.prebid.bidder.rubicon.accountId = bidderAliasParam
            ext.prebid.floors.floorMin = FLOOR_MIN
        }
        def storedRequest = StoredRequest.getDbStoredRequest(ampRequest, ampStoredRequest)
        storedRequestDao.save(storedRequest)

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(ampRequest.account as String)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorValue]
            modelGroups[0].currency = USD
        }
        floorsProvider.setResponse(ampRequest.account as String, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(ampRequest)

        and: "Bid response for rubicon bidder"
        def bidResponse = BidResponse.getDefaultBidResponse(ampStoredRequest).tap {
            seatbid.first().bid.first().price = floorValue
        }
        bidder.setResponse(bidder.getRequest(bidderParam as String, "site.publisher.ext.rp.account_id"), bidResponse)

        and: "Bid response for rubicon bidder alias"
        def lowerPrice = floorValue - 0.1
        def aliasBidResponse = BidResponse.getDefaultBidResponse(ampStoredRequest).tap {
            seatbid.first().bid.first().price = lowerPrice
        }
        bidder.setResponse(bidder.getRequest(bidderAliasParam as String, "site.publisher.ext.rp.account_id"), aliasBidResponse)

        when: "PBS processes amp request"
        floorsPbsService.sendAmpRequest(ampRequest)

        then: "Analytics request should contain floor data from floors provider, information about rejected bid"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(ampStoredRequest.id).last()
        def winningBid = analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.findAll { it.status == Status.SUCCESS }
        def losingBid = analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.findAll { it.status == REJECTED_IPF }
        verifyAll(analyticsRequest) {
            winningBid?.size() == 1
            losingBid?.size() == 1
            winningBid.first().bidResponse?.bidPriceUsd == floorValue as Float
            !losingBid.first().bidResponse?.bidPriceUsd
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorValue } ==
                    [floorValue, floorValue]
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorRule } ==
                    [rule, rule]
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorRuleValue } ==
                    [floorValue, floorValue]

            auctions?.first()?.floors?.location == FETCH
            auctions?.first()?.floors?.fetchStatus == SUCCESS
            !auctions?.first()?.floors?.skipped
            !auctions?.first()?.floors?.modelName
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            auctions?.first()?.floors?.skipRate == floorsResponse.skipRate
            auctions?.first()?.floors?.provider == floorsResponse.floorProvider
            auctions?.first()?.floors?.floorMin == ampStoredRequest.ext.prebid.floors.floorMin
            !auctions?.first()?.floors?.modelTimestamp
            !auctions?.first()?.floors?.modelWeight
        }
    }

    def "PBS should pass floors information after currency conversion when floorValue != USD"() {
        given: "Rubicon BidRequest with floorMinCur"
        def floorMin = PBSUtils.randomFloorValue
        def bidRequest = bidRequestWithFloors.tap {
            cur = [EUR]
            imp[0].bidFloor = floorMin
            imp[0].bidFloorCur = EUR
            ext.prebid.floors.floorMin = floorMin
            ext.prebid.floors.floorMinCur = USD
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id)
        accountDao.save(account)

        and: "Set Floors Provider response with a currency different from the floorMinCur, floorValue lower then floorMin"
        def floorProviderCur = EUR
        def floorMinFloorProviderCur = convertCurrency(floorMin, bidRequest.ext.prebid.floors.floorMinCur, floorProviderCur)
        def floorRuleValueEur = floorMinFloorProviderCur - 0.1
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorRuleValueEur]
            modelGroups[0].currency = floorProviderCur
        }
        floorsProvider.setResponse(bidRequest.app.publisher.id, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        and: "Set bidder response"
        def signallingFloorValue = convertCurrency(floorMinFloorProviderCur, floorProviderCur, DEFAULT_CURRENCY)
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().price = signallingFloorValue
            cur = USD
        }
        bidder.setResponse(bidRequest.id, bidResponse)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data after currency conversion"
        def floorRuleValueUsd = convertCurrency(floorRuleValueEur, floorProviderCur, USD)
        def bidPriceEur = convertCurrency(bidResponse.seatbid.first().bid.first().price, bidResponse.cur, bidRequest.cur.first())
        def bidPriceUsd = convertCurrency(bidPriceEur, bidRequest.cur.first(), DEFAULT_CURRENCY)

        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(bidRequest.id).last()
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids[0].status == Status.SUCCESS
            auctions?.first()?.adUnits?.first()?.bids[0].bidResponse?.bidPriceUsd == bidPriceUsd as Float

            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorValue == signallingFloorValue
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRule ==
                    floorsResponse.modelGroups[0].values.keySet()[0]
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRuleValue == floorRuleValueUsd

            auctions?.first()?.floors?.location == FETCH
            auctions?.first()?.floors?.fetchStatus == SUCCESS
            !auctions?.first()?.floors?.skipped
            !auctions?.first()?.floors?.modelName
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            auctions?.first()?.floors?.skipRate == floorsResponse.skipRate
            auctions?.first()?.floors?.provider == floorsResponse.floorProvider
            auctions?.first()?.floors?.floorMin == floorMin
            !auctions?.first()?.floors?.modelTimestamp
            !auctions?.first()?.floors?.modelWeight
        }
    }

    def "PBS should pass floors information after currency conversion when floorValue != USD for amp request"() {
        given: "Default AmpRequest"
        def ampRequest = AmpRequest.defaultAmpRequest

        and: "Default stored request with floors "
        def floorMin = PBSUtils.randomFloorValue
        def ampStoredRequest = rubiconStoredRequestWithFloors.tap {
            cur = [EUR]
            imp[0].bidFloor = floorMin
            imp[0].bidFloorCur = EUR
            ext.prebid.floors.floorMin = floorMin
            ext.prebid.floors.floorMinCur = USD
        }
        def storedRequest = StoredRequest.getDbStoredRequest(ampRequest, ampStoredRequest)
        storedRequestDao.save(storedRequest)

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(ampRequest.account as String)
        accountDao.save(account)

        and: "Set Floors Provider response with a currency different from the floorMinCur, floorValue lower then floorMin"
        def floorProviderCur = EUR
        def floorMinFloorProviderCur = convertCurrency(floorMin, ampStoredRequest.ext.prebid.floors.floorMinCur, floorProviderCur)
        def floorRuleValueEur = floorMinFloorProviderCur - 0.1
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorRuleValueEur]
            modelGroups[0].currency = floorProviderCur
        }
        floorsProvider.setResponse(ampRequest.account as String, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(ampRequest)

        and: "Set bidder response"
        def signallingFloorValue = convertCurrency(floorMinFloorProviderCur, floorProviderCur, DEFAULT_CURRENCY)
        def bidResponse = BidResponse.getDefaultBidResponse(ampStoredRequest).tap {
            seatbid.first().bid.first().price = signallingFloorValue
            cur = USD
        }
        bidder.setResponse(ampStoredRequest.id, bidResponse)

        when: "PBS processes amp request"
        floorsPbsService.sendAmpRequest(ampRequest)

        then: "Analytics request should contain floor data after currency conversion"
        def floorRuleValueUsd = convertCurrency(floorRuleValueEur, floorProviderCur, USD)
        def bidPriceEur = convertCurrency(bidResponse.seatbid.first().bid.first().price, bidResponse.cur, ampStoredRequest.cur.first())
        def bidPriceUsd = convertCurrency(bidPriceEur, ampStoredRequest.cur.first(), DEFAULT_CURRENCY)
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(ampStoredRequest.id).last()
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids[0].status == Status.SUCCESS
            auctions?.first()?.adUnits?.first()?.bids[0].bidResponse?.bidPriceUsd == bidPriceUsd as Float

            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorValue == signallingFloorValue
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRule ==
                    floorsResponse.modelGroups[0].values.keySet()[0]
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRuleValue == floorRuleValueUsd

            auctions?.first()?.floors?.location == FETCH
            auctions?.first()?.floors?.fetchStatus == SUCCESS
            !auctions?.first()?.floors?.skipped
            !auctions?.first()?.floors?.modelName
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            auctions?.first()?.floors?.skipRate == floorsResponse.skipRate
            auctions?.first()?.floors?.provider == floorsResponse.floorProvider
            auctions?.first()?.floors?.floorMin == floorMin
            !auctions?.first()?.floors?.modelTimestamp
            !auctions?.first()?.floors?.modelWeight
        }
    }

    def "PBS should pass floors information after bidAdjustment when adjustForBidAdjustment = true"() {
        given: "Rubicon BidRequest with bidAdjustment"
        def floorsProviderFloorValue = 0.8
        def bidAdjustment = 0.1
        def bidRequest = bidRequestWithFloors.tap {
            ext.prebid.floors.enforcement = new ExtPrebidPriceFloorEnforcement(bidAdjustment: true)
            ext.prebid.bidAdjustmentFactors = new BidAdjustmentFactors(adjustments: [(RUBICON): bidAdjustment as BigDecimal])
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorsProviderFloorValue]
        }
        floorsProvider.setResponse(bidRequest.app.publisher.id, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        and: "Set bidder response"
        def adjustedPrice = floorsProviderFloorValue / bidAdjustment
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().price = adjustedPrice
        }
        bidder.setResponse(bidRequest.id, bidResponse)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data after adjustment"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(bidRequest.id).last()
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids[0].status == Status.SUCCESS
            auctions?.first()?.adUnits?.first()?.bids[0].bidResponse?.bidPriceUsd == floorsProviderFloorValue as Float

            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorValue == floorsProviderFloorValue
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRule ==
                    floorsResponse.modelGroups[0].values.keySet()[0]
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRuleValue == floorsProviderFloorValue
        }
    }

    def "PBS should pass floors information after bidAdjustment when adjustForBidAdjustment = true for amp request"() {
        given: "Default AmpRequest"
        def ampRequest = AmpRequest.defaultAmpRequest

        and: "Default stored request with floors, bidAdjustment"
        def floorsProviderFloorValue = 0.8
        def bidAdjustment = 0.1
        def ampStoredRequest = rubiconStoredRequestWithFloors.tap {
            ext.prebid.floors.enforcement = new ExtPrebidPriceFloorEnforcement(bidAdjustment: true)
            ext.prebid.bidAdjustmentFactors = new BidAdjustmentFactors(adjustments: [(RUBICON): bidAdjustment as BigDecimal])
        }
        def storedRequest = StoredRequest.getDbStoredRequest(ampRequest, ampStoredRequest)
        storedRequestDao.save(storedRequest)

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(ampRequest.account as String)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorsProviderFloorValue]
        }
        floorsProvider.setResponse(ampRequest.account as String, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(ampRequest)

        and: "Set bidder response"
        def adjustedPrice = floorsProviderFloorValue / bidAdjustment
        def bidResponse = BidResponse.getDefaultBidResponse(ampStoredRequest).tap {
            seatbid.first().bid.first().price = adjustedPrice
        }
        bidder.setResponse(ampStoredRequest.id, bidResponse)

        when: "PBS processes amp request"
        floorsPbsService.sendAmpRequest(ampRequest)

        then: "Analytics request should contain floor data after adjustment"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(ampStoredRequest.id).last()
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids[0].status == Status.SUCCESS
            auctions?.first()?.adUnits?.first()?.bids[0].bidResponse?.bidPriceUsd == floorsProviderFloorValue as Float

            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorValue == floorsProviderFloorValue
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRule ==
                    floorsResponse.modelGroups[0].values.keySet()[0]
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRuleValue == floorsProviderFloorValue
        }
    }

    def "PBS should pass floors information from request when fetch is disabled"() {
        given: "Rubicon BidRequest with floors, storedRequest"
        def bidRequest = bidRequestWithFloors.tap {
            ext.prebid.storedRequest = new PrebidStoredRequest(id: PBSUtils.randomNumber)
        }

        and: "Save storedRequest with floors into DB"
        def storedRequestModel = rubiconStoredRequestWithFloors
        def storedRequest = StoredRequest.getDbStoredRequest(bidRequest, storedRequestModel, bidRequest.app.publisher.id)
        storedRequestDao.save(storedRequest)

        and: "Account with disabled fetch in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id).tap {
            config.auction.priceFloors.fetch.enabled = false
        }
        accountDao.save(account)

        and: "Set bidder response"
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().price = bidRequest.imp[0].bidFloor
        }
        bidder.setResponse(bidRequest.id, bidResponse)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data from request"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequest(bidRequest.id)
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids[0].status == Status.SUCCESS
            auctions?.first()?.adUnits?.first()?.bids[0].bidResponse?.bidPriceUsd ==
                    bidResponse.seatbid.first().bid.first().price as Float

            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorValue == bidRequest.imp[0].bidFloor
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRule ==
                    bidRequest.ext.prebid.floors.data.modelGroups[0].values.keySet()[0]
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRuleValue ==
                    bidRequest.imp[0].bidFloor

            auctions?.first()?.floors?.location == REQUEST
            auctions?.first()?.floors?.fetchStatus == NONE
            auctions?.first()?.floors?.skipped == bidRequest.ext.prebid.floors.skipped
            auctions?.first()?.floors?.modelName == bidRequest.ext.prebid.floors.data.modelGroups[0].modelVersion
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            auctions?.first()?.floors?.skipRate == bidRequest.ext.prebid.floors.skipRate
            auctions?.first()?.floors?.provider == bidRequest.ext.prebid.floors.data.floorProvider
            auctions?.first()?.floors?.floorMin == bidRequest.ext.prebid.floors.floorMin
            auctions?.first()?.floors?.modelTimestamp == bidRequest.ext.prebid.floors.data.modelTimestamp
            auctions?.first()?.floors?.modelWeight == bidRequest.ext.prebid.floors.data.modelGroups[0].modelWeight
        }
    }

    def "PBS should pass floors information from request when fetch is disabled for amp request"() {
        given: "Default AmpRequest"
        def ampRequest = AmpRequest.defaultAmpRequest

        and: "Default stored request with floors "
        def floorValue = PBSUtils.randomFloorValue
        def ampStoredRequest = rubiconStoredRequestWithFloors.tap {
            ext.prebid.floors.data.modelGroups.first().values = [(rule): floorValue]
        }
        def storedRequest = StoredRequest.getDbStoredRequest(ampRequest, ampStoredRequest)
        storedRequestDao.save(storedRequest)

        and: "Account with disabled fetch in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(ampRequest.account as String).tap {
            config.auction.priceFloors.fetch.enabled = false
        }
        accountDao.save(account)

        and: "Set bidder response"
        def bidResponse = BidResponse.getDefaultBidResponse(ampStoredRequest).tap {
            seatbid.first().bid.first().price = floorValue
        }
        bidder.setResponse(ampStoredRequest.id, bidResponse)

        when: "PBS processes amp request"
        floorsPbsService.sendAmpRequest(ampRequest)

        then: "Analytics request should contain floor data from stored request"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequest(ampStoredRequest.id)
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids[0].status == Status.SUCCESS
            auctions?.first()?.adUnits?.first()?.bids[0].bidResponse?.bidPriceUsd ==
                    bidResponse.seatbid.first().bid.first().price as Float

            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorValue == floorValue
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRule ==
                    ampStoredRequest.ext.prebid.floors.data.modelGroups[0].values.keySet()[0]
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRuleValue == floorValue

            auctions?.first()?.floors?.location == REQUEST
            auctions?.first()?.floors?.fetchStatus == NONE
            auctions?.first()?.floors?.skipped == ampStoredRequest.ext.prebid.floors.skipped
            auctions?.first()?.floors?.modelName == ampStoredRequest.ext.prebid.floors.data.modelGroups[0].modelVersion
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            auctions?.first()?.floors?.skipRate == ampStoredRequest.ext.prebid.floors.skipRate
            auctions?.first()?.floors?.provider == ampStoredRequest.ext.prebid.floors.data.floorProvider
            auctions?.first()?.floors?.floorMin == ampStoredRequest.ext.prebid.floors.floorMin
            auctions?.first()?.floors?.modelTimestamp == ampStoredRequest.ext.prebid.floors.data.modelTimestamp
            auctions?.first()?.floors?.modelWeight == ampStoredRequest.ext.prebid.floors.data.modelGroups[0].modelWeight
        }
    }

    def "PBS should pass dealsEnforced = true to analytics adapter when floorDeals = true"() {
        given: "Rubicon BidRequest with floors, preferdeals = true"
        def bidRequest = bidRequestWithFloors.tap {
            ext.prebid.targeting = new Targeting(preferdeals: true)
            ext.prebid.floors.enforcement = new ExtPrebidPriceFloorEnforcement(floorDeals: true)
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorValue]
        }
        floorsProvider.setResponse(bidRequest.app.publisher.id, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        and: "Bid response with 2 bids: bid.price = floorValue, dealBid.price < floorValue"
        def nonDealBidPrice = floorValue
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid << Bid.getDefaultBid(bidRequest.imp.first())
            seatbid.first().bid.first().dealid = PBSUtils.randomNumber
            seatbid.first().bid.first().price = floorValue - 0.1
            seatbid.first().bid.last().price = floorValue
        }

        and: "Set bidder response"
        bidder.setResponse(bidRequest.id, bidResponse)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data for bid without deal"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(bidRequest.id).last()
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.bidPriceUsd } ==
                    [nonDealBidPrice as Float]
            auctions?.first()?.floors?.dealsEnforced
        }
    }

    def "PBS should pass dealsEnforced = false to analytics adapter when floorDeals = false"() {
        given: "Rubicon BidRequest with floors"
        def bidRequest = bidRequestWithFloors.tap {
            ext.prebid.targeting = new Targeting(preferdeals: true)
            ext.prebid.floors.enforcement = new ExtPrebidPriceFloorEnforcement(floorDeals: false)
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorValue]
        }
        floorsProvider.setResponse(bidRequest.app.publisher.id, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        and: "Bid response with 2 bids: bid.price = floorValue, dealBid.price < floorValue"
        def dealBidPrice = floorValue - 0.1
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid << Bid.getDefaultBid(bidRequest.imp.first())
            seatbid.first().bid.first().dealid = PBSUtils.randomNumber
            seatbid.first().bid.first().price = dealBidPrice
            seatbid.first().bid.last().price = floorValue
        }

        and: "Set bidder response"
        bidder.setResponse(bidRequest.id, bidResponse)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data for bid with deal"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(bidRequest.id).last()
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.bidPriceUsd } == [dealBidPrice as Float]
            !auctions?.first()?.floors?.dealsEnforced
        }
    }

    def "PBS should not make PF signalling, enforcing when skipRate = #skipRate"() {
        given: "Rubicon BidRequest with bidFloor, bidFloorCur"
        def bidRequest = bidRequestWithFloors.tap {
            imp[0].bidFloor = PBSUtils.randomFloorValue
            imp[0].bidFloorCur = USD
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id)
        accountDao.save(account)

        and: "Set Floors Provider response with skipRate"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorValue]
            modelGroups[0].currency = USD
            modelGroups[0].skipRate = 100
        }
        floorsProvider.setResponse(bidRequest.app.publisher.id, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data from stored request"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(bidRequest.id).last()
        verifyAll(analyticsRequest) {
            auctions?.first()?.adUnits?.first()?.bids?.first()?.status == Status.SUCCESS

            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorValue == floorValue
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRule ==
                    floorsResponse.modelGroups[0].values.keySet()[0]
            auctions?.first()?.adUnits?.first()?.bids?.first()?.bidResponse?.floorRuleValue == floorValue

            auctions?.first()?.floors?.location == FETCH
            auctions?.first()?.floors?.fetchStatus == SUCCESS
            auctions?.first()?.floors?.skipped
            auctions?.first()?.floors?.skipRate == floorsResponse.modelGroups[0].skipRate
        }
    }

    def "PBS should pass modelName, provider, modelTimestamp, modelWeight when data is defined"() {
        given: "Rubicon BidRequest"
        def bidRequest = rubiconBidRequest

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].values = [(rule): floorValue]
            modelGroups[0].modelVersion = PBSUtils.randomString
            modelGroups[0].modelWeight = PriceFloorsBaseSpec.modelWeight
            floorProvider = PBSUtils.randomString
            modelTimestamp = PBSUtils.randomNumber
        }
        floorsProvider.setResponse(bidRequest.app.publisher.id, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        and: "Bid response"
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().price = floorValue
        }
        bidder.setResponse(bidRequest.id, bidResponse)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain modelName, provider, modelTimestamp, modelWeight from floors provider"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(bidRequest.id).last()
        verifyAll(analyticsRequest) {
            auctions?.first()?.floors?.modelName == floorsResponse.modelGroups[0].modelVersion
            auctions?.first()?.floors?.provider == floorsResponse.floorProvider
            auctions?.first()?.floors?.modelTimestamp == floorsResponse.modelTimestamp
            auctions?.first()?.floors?.modelWeight == floorsResponse.modelGroups[0].modelWeight
        }
    }

    def "PBS should pass information about rejection when PF enforcement was applied according to getFloors() function for auction request"() {
        given: "Rubicon BidRequest with rubicon bidder, rubicon alias bidder"
        def alias = "rubiconAlias"
        def bidderParam = PBSUtils.randomNumber
        def bidderAliasParam = PBSUtils.randomNumber
        def bidRequest = rubiconBidRequest.tap {
            ext.prebid.aliases = [(alias): RUBICON]
            imp[0].ext.prebid.bidder.rubiconAlias = Rubicon.default.tap { accountId = bidderParam }
            imp[0].ext.prebid.bidder.rubicon.accountId = bidderAliasParam
            imp[0].video = rubiconVideoImp
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(bidRequest.app.publisher.id)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def videoMediaTypeRule = new Rule(mediaType: VIDEO).rule
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].schema = new PriceFloorSchema(fields: [MEDIA_TYPE])
            modelGroups[0].values =
                    [(new Rule(mediaType: MediaType.MULTIPLE).rule): floorValue + 0.2,
                     (new Rule(mediaType: BANNER).rule)            : floorValue + 0.1,
                     (videoMediaTypeRule)                          : floorValue]
        }
        floorsProvider.setResponse(bidRequest.app.publisher.id, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        and: "Bid response for rubicon bidder"
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().price = floorValue
        }
        bidder.setResponse(bidder.getRequest(bidderParam as String, "app.publisher.ext.rp.account_id"), bidResponse)

        and: "Bid response for rubicon bidder alias"
        def losingBidPrice = floorValue - 0.1
        def aliasBidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid.first().price = losingBidPrice
        }
        bidder.setResponse(bidder.getRequest(bidderAliasParam as String, "app.publisher.ext.rp.account_id"), aliasBidResponse)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Analytics request should contain floor data from floors provider, information about rejected bid"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(bidRequest.id).last()
        def winningBid = analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.findAll { it.status == Status.SUCCESS }
        def losingBid = analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.findAll { it.status == REJECTED_IPF }
        verifyAll(analyticsRequest) {
            winningBid?.size() == 1
            losingBid?.size() == 1
            winningBid.first().bidResponse?.bidPriceUsd == floorValue as Float
            !losingBid.first().bidResponse?.bidPriceUsd
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorValue } ==
                    [floorValue, floorValue]
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorRule } ==
                    [videoMediaTypeRule, videoMediaTypeRule]
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorRuleValue } ==
                    [floorValue, floorValue]

            auctions?.first()?.floors?.location == FETCH
            auctions?.first()?.floors?.fetchStatus == SUCCESS
            !auctions?.first()?.floors?.skipped
            !auctions?.first()?.floors?.modelName
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            auctions?.first()?.floors?.skipRate == floorsResponse.skipRate
            auctions?.first()?.floors?.provider == floorsResponse.floorProvider
            !auctions?.first()?.floors?.floorMin
            !auctions?.first()?.floors?.modelTimestamp
            !auctions?.first()?.floors?.modelWeight
        }
    }

    def "PBS should pass information about rejection when PF enforcement was applied according to getFloors() function for amp request"() {
        given: "Default AmpRequest"
        def ampRequest = AmpRequest.defaultAmpRequest

        and: "Default stored request with floors "
        def alias = "rubiconAlias"
        def bidderParam = PBSUtils.randomNumber
        def bidderAliasParam = PBSUtils.randomNumber
        def ampStoredRequest = rubiconStoredRequestWithFloors.tap {
            ext.prebid.aliases = [(alias): RUBICON]
            imp[0].ext.prebid.bidder.rubiconAlias = Rubicon.default.tap { accountId = bidderParam }
            imp[0].ext.prebid.bidder.rubicon.accountId = bidderAliasParam
            ext.prebid.floors.floorMin = FLOOR_MIN
            imp[0].video = rubiconVideoImp
        }
        def storedRequest = StoredRequest.getDbStoredRequest(ampRequest, ampStoredRequest)
        storedRequestDao.save(storedRequest)

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(ampRequest.account as String)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def videoMediaTypeRule = new Rule(mediaType: VIDEO).rule
        def floorsResponse = PriceFloorData.priceFloorData.tap {
            modelGroups[0].schema = new PriceFloorSchema(fields: [MEDIA_TYPE])
            modelGroups[0].values =
                    [(new Rule(mediaType: MediaType.MULTIPLE).rule): floorValue + 0.2,
                     (new Rule(mediaType: BANNER).rule)            : floorValue + 0.1,
                     (videoMediaTypeRule)                          : floorValue]
        }
        floorsProvider.setResponse(ampRequest.account as String, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(ampRequest)

        and: "Bid response for rubicon bidder"
        def bidResponse = BidResponse.getDefaultBidResponse(ampStoredRequest).tap {
            seatbid.first().bid.first().price = floorValue
        }
        bidder.setResponse(bidder.getRequest(bidderParam as String, "site.publisher.ext.rp.account_id"), bidResponse)

        and: "Bid response for rubicon bidder alias"
        def lowerPrice = floorValue - 0.1
        def aliasBidResponse = BidResponse.getDefaultBidResponse(ampStoredRequest).tap {
            seatbid.first().bid.first().price = lowerPrice
        }
        bidder.setResponse(bidder.getRequest(bidderAliasParam as String, "site.publisher.ext.rp.account_id"), aliasBidResponse)

        when: "PBS processes amp request"
        floorsPbsService.sendAmpRequest(ampRequest)

        then: "Analytics request should contain floor data from floors provider, information about rejected bid"
        def analyticsRequest = rubiconAnalytics.getAnalyticsRequests(ampStoredRequest.id).last()
        def winningBid = analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.findAll { it.status == Status.SUCCESS }
        def losingBid = analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.findAll { it.status == REJECTED_IPF }
        verifyAll(analyticsRequest) {
            winningBid?.size() == 1
            losingBid?.size() == 1
            winningBid.first().bidResponse?.bidPriceUsd == floorValue as Float
            !losingBid.first().bidResponse?.bidPriceUsd
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorValue } ==
                    [floorValue, floorValue]
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorRule } ==
                    [videoMediaTypeRule, videoMediaTypeRule]
            analyticsRequest.auctions?.first()?.adUnits?.first()?.bids?.collect { it.bidResponse.floorRuleValue } ==
                    [floorValue, floorValue]

            auctions?.first()?.floors?.location == FETCH
            auctions?.first()?.floors?.fetchStatus == SUCCESS
            !auctions?.first()?.floors?.skipped
            !auctions?.first()?.floors?.modelName
            !auctions?.first()?.floors?.enforcement
            !auctions?.first()?.floors?.dealsEnforced
            auctions?.first()?.floors?.skipRate == floorsResponse.skipRate
            auctions?.first()?.floors?.provider == floorsResponse.floorProvider
            auctions?.first()?.floors?.floorMin == ampStoredRequest.ext.prebid.floors.floorMin
            !auctions?.first()?.floors?.modelTimestamp
            !auctions?.first()?.floors?.modelWeight
        }
    }

    private static BidRequest getBidRequestWithFloors(DistributionChannel channel = APP) {
        def floors = ExtPrebidFloors.extPrebidFloors
        getRubiconBidRequest(channel).tap {
            imp[0].bidFloor = floors.data.modelGroups[0].values[rule]
            imp[0].bidFloorCur = floors.data.modelGroups[0].currency
            ext.prebid.floors = floors
        }
    }
}
