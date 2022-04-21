package org.prebid.server.functional.tests.rubicon.pricefloors

import org.prebid.server.functional.model.db.StoredRequest
import org.prebid.server.functional.model.mock.services.floorsprovider.PriceFloorRules
import org.prebid.server.functional.model.pricefloors.MediaType
import org.prebid.server.functional.model.pricefloors.PriceFloorSchema
import org.prebid.server.functional.model.pricefloors.Rule
import org.prebid.server.functional.model.request.amp.AmpRequest
import org.prebid.server.functional.model.request.auction.App
import org.prebid.server.functional.model.request.auction.BidRequest
import org.prebid.server.functional.model.request.auction.Site
import org.prebid.server.functional.model.response.auction.Bid
import org.prebid.server.functional.model.response.auction.BidResponse
import org.prebid.server.functional.service.PrebidServerService
import org.prebid.server.functional.testcontainers.PbsConfig
import org.prebid.server.functional.tests.pricefloors.PriceFloorsBaseSpec
import org.prebid.server.functional.tests.rubicon.RubiconBaseSpec
import org.prebid.server.functional.util.PBSUtils
import spock.lang.Shared

import static org.prebid.server.functional.model.pricefloors.MediaType.BANNER
import static org.prebid.server.functional.model.pricefloors.MediaType.VIDEO
import static org.prebid.server.functional.model.pricefloors.PriceFloorField.MEDIA_TYPE
import static org.prebid.server.functional.model.response.auction.ErrorType.RUBICON
import static org.prebid.server.functional.tests.pricefloors.PriceFloorsBaseSpec.floorsConfig

class RubiconFloorsSpec extends RubiconBaseSpec {

    @Shared
    final PrebidServerService floorsPbsService = pbsServiceFactory.getService(
            RUBICON_CONFIG + floorsConfig + PbsConfig.externalCurrencyConverterConfig)

    def "PBS should prefer video when both banner and video objects are present in an imp for auction request"() {
        given: "Rubicon BidRequest with banner, video"
        def accountId = PBSUtils.randomNumber.toString()
        def bidRequest = rubiconBidRequest.tap {
            updateBidRequestClosure(it, accountId)
            imp[0].video = rubiconVideoImp
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(accountId)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorRules.priceFloorRules.tap {
            data.modelGroups[0].schema = new PriceFloorSchema(fields: [MEDIA_TYPE])
            data.modelGroups[0].values =
                    [(new Rule(mediaType: MediaType.MULTIPLE).rule): floorValue + 0.2,
                     (new Rule(mediaType: BANNER).rule)            : floorValue + 0.1,
                     (new Rule(mediaType: VIDEO).rule)             : floorValue]
        }
        floorsProvider.setResponse(accountId, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        when: "PBS processes auction request"
        floorsPbsService.sendAuctionRequest(bidRequest)

        then: "Bidder request bidFloor should correspond to video media type"
        def bidderRequest = bidder.getBidderRequests(bidRequest.id).last()
        assert bidderRequest.imp[0].bidFloor == floorValue

        and: "Adapter is not passing ext.prebid.floors to XAPI"
        assert !bidderRequest.ext?.prebid?.floors

        where:
        updateBidRequestClosure << [{BidRequest request, String accountIdVal -> {
                                    request.site = Site.defaultSite.tap {publisher.id = accountIdVal }
                                    request.app = null} },
                                    {BidRequest request, String accountIdVal -> {
                                    request.site = null
                                    request.app = App.defaultApp.tap {publisher.id = accountIdVal }}}]
    }

    def "PBS should prefer video when both banner and video objects are present in an imp for amp request"() {
        given: "Default AmpRequest"
        def ampRequest = AmpRequest.defaultAmpRequest

        and: "Rubicon stored request with banner, video"
        def ampStoredRequest = rubiconStoredRequestWithFloors.tap { imp[0].video = rubiconVideoImp }

        and: "Save storedRequest into DB"
        def storedRequest = StoredRequest.getDbStoredRequest(ampRequest, ampStoredRequest)
        storedRequestDao.save(storedRequest)

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(ampRequest.account.toString())
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorRules.priceFloorRules.tap {
            data.modelGroups[0].schema = new PriceFloorSchema(fields: [MEDIA_TYPE])
            data.modelGroups[0].values =
                    [(new Rule(mediaType: MediaType.MULTIPLE).rule): floorValue + 0.2,
                     (new Rule(mediaType: BANNER).rule)            : floorValue + 0.1,
                     (new Rule(mediaType: VIDEO).rule)             : floorValue]
        }
        floorsProvider.setResponse(ampRequest.account.toString(), floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(ampRequest)

        when: "PBS processes auction request"
        floorsPbsService.sendAmpRequest(ampRequest)

        then: "Bidder request bidFloor should correspond to video media type"
        def bidderRequest = bidder.getBidderRequests(ampStoredRequest.id).last()
        assert bidderRequest.imp[0].bidFloor == floorValue

        and: "Adapter is not passing ext.prebid.floors to XAPI"
        assert !bidderRequest.ext?.prebid?.floors
    }

    def "PBS should make PF enforcement for auction request when getFloors() function defined for bidder"() {
        given: "Rubicon BidRequest with banner, video"
        def accountId = PBSUtils.randomNumber.toString()
        def bidRequest = rubiconBidRequest.tap {
            updateBidRequestClosure(it, accountId)
            imp[0].video = rubiconVideoImp
        }

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(accountId)
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorRules.priceFloorRules.tap {
            data.modelGroups[0].schema = new PriceFloorSchema(fields: [MEDIA_TYPE])
            data.modelGroups[0].values =
                    [(new Rule(mediaType: MediaType.MULTIPLE).rule): floorValue + 0.2,
                     (new Rule(mediaType: BANNER).rule)            : floorValue + 0.1,
                     (new Rule(mediaType: VIDEO).rule)             : floorValue]
        }
        floorsProvider.setResponse(accountId, floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(bidRequest)

        and: "Bid response with 2 bids: price = floorValue, price < floorValue"
        def bidResponse = BidResponse.getDefaultBidResponse(bidRequest).tap {
            seatbid.first().bid << Bid.getDefaultBid(bidRequest.imp.first())
            seatbid.first().bid.first().price = floorValue
            seatbid.first().bid.last().price = floorValue - 0.1
        }
        bidder.setResponse(bidRequest.id, bidResponse)

        when: "PBS processes auction request"
        def response= floorsPbsService.sendAuctionRequest(bidRequest)

        then: "PBS should suppress all bids below the floor value"
        assert response.seatbid?.first()?.bid?.collect { it.price } == [floorValue]

        and: "PBS should log warning about suppression"
        assert response.ext?.warnings[RUBICON]*.code == [6]
        assert response.ext?.warnings[RUBICON]*.message ==
                ["Bid with id '${bidResponse.seatbid[0].bid[1].id}' was rejected by floor enforcement: " +
                         "price ${bidResponse.seatbid[0].bid[1].price} is below the floor $floorValue" as String]

        where:
        updateBidRequestClosure << [{BidRequest request, String accountIdVal -> {
                                    request.site = Site.defaultSite.tap {publisher.id = accountIdVal }
                                    request.app = null} },
                                    {BidRequest request, String accountIdVal -> {
                                    request.site = null
                                    request.app = App.defaultApp.tap {publisher.id = accountIdVal }}}]
    }

    def "PBS should make PF enforcement for amp request when getFloors() function defined for bidder"() {
        given: "Default AmpRequest"
        def ampRequest = AmpRequest.defaultAmpRequest

        and: "Rubicon stored request with banner, video"
        def ampStoredRequest = rubiconStoredRequestWithFloors.tap { imp[0].video = rubiconVideoImp }

        and: "Save storedRequest into DB"
        def storedRequest = StoredRequest.getDbStoredRequest(ampRequest, ampStoredRequest)
        storedRequestDao.save(storedRequest)

        and: "Account with enabled fetch, fetch.url in the DB"
        def account = PriceFloorsBaseSpec.getAccountWithEnabledFetch(ampRequest.account.toString())
        accountDao.save(account)

        and: "Set Floors Provider response"
        def floorValue = PBSUtils.randomFloorValue
        def floorsResponse = PriceFloorRules.priceFloorRules.tap {
            data.modelGroups[0].schema = new PriceFloorSchema(fields: [MEDIA_TYPE])
            data.modelGroups[0].values =
                    [(new Rule(mediaType: MediaType.MULTIPLE).rule): floorValue + 0.2,
                     (new Rule(mediaType: BANNER).rule)            : floorValue + 0.1,
                     (new Rule(mediaType: VIDEO).rule)             : floorValue]
        }
        floorsProvider.setResponse(ampRequest.account.toString(), floorsResponse)

        and: "PBS fetch rules from floors provider"
        cacheFloorsProviderRules(ampRequest)

        and: "Bid response with 2 bids: price = floorValue, price < floorValue"
        def bidResponse = BidResponse.getDefaultBidResponse(ampStoredRequest).tap {
            seatbid.first().bid << Bid.getDefaultBid(ampStoredRequest.imp.first())
            seatbid.first().bid.first().price = floorValue
            seatbid.first().bid.last().price = floorValue - 0.1
        }
        bidder.setResponse(ampStoredRequest.id, bidResponse)

        when: "PBS processes auction request"
        def response = floorsPbsService.sendAmpRequest(ampRequest)

        then: "PBS should suppress all bids below the floor value"
        def bidPrice = getRoundedTargetingValueWithDefaultPrecision(floorValue)
        assert response.targeting["hb_pb"] == bidPrice

        and: "PBS should log warning about suppression"
        assert response.ext?.warnings[RUBICON]*.code == [6]
        assert response.ext?.warnings[RUBICON]*.message ==
                ["Bid with id '${bidResponse.seatbid[0].bid[1].id}' was rejected by floor enforcement: " +
                         "price ${bidResponse.seatbid[0].bid[1].price} is below the floor $floorValue" as String]
    }
}
