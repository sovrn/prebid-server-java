package org.prebid.server.functional.tests.rubicon

import org.prebid.server.functional.model.bidder.Rubicon
import org.prebid.server.functional.model.request.auction.BidRequest
import org.prebid.server.functional.model.request.auction.DistributionChannel
import org.prebid.server.functional.service.PrebidServerService
import org.prebid.server.functional.testcontainers.Dependencies
import org.prebid.server.functional.testcontainers.PBSTest
import org.prebid.server.functional.testcontainers.scaffolding.RubiconAnalytics
import org.prebid.server.functional.tests.BaseSpec
import org.prebid.server.functional.util.PBSUtils

import static org.prebid.server.functional.model.request.auction.DistributionChannel.APP
import static org.prebid.server.functional.testcontainers.Dependencies.networkServiceContainer

@PBSTest
abstract class RubiconBaseSpec extends BaseSpec {

    protected static final RubiconAnalytics rubiconAnalytics = new RubiconAnalytics(networkServiceContainer, Dependencies.objectMapperWrapper)

    protected static final Map<String, String> RUBICON_CONFIG =
            ["gdpr.rubicon.rsid-cookie-encryption-key" : PBSUtils.randomString,
             "gdpr.rubicon.audit-cookie-encryption-key": PBSUtils.randomString,
             "gdpr.host-vendor-id"                     : RUBICON_VENDOR_ID,
             "auction.enforce-random-bid-id"           : "false",
             "host-cookie.domain"                      : PBSUtils.randomString,
             "host-cookie.ttl-days"                    : "60",
             "adapters.rubicon.enabled"                : "true",
             "adapters.rubicon.endpoint"               : "$networkServiceContainer.rootUri/auction" as String]

    protected static final Map<String, String> ANALYTICS_CONFIG =
            ["analytics.rp.enabled"        : "true",
             "analytics.rp.host-url"       : networkServiceContainer.rootUri,
             "analytics.rp.host-vendor-id" : RUBICON_VENDOR_ID,
             "analytics.rp.pbs-version"    : "0.0.1",
             "analytics.rp.sampling-factor": "1"]

    protected static final PrebidServerService rubiconPbsService = pbsServiceFactory.getService(RUBICON_CONFIG +
            ANALYTICS_CONFIG)

    private static final String RUBICON_VENDOR_ID = "52"

    protected static BidRequest getRubiconBidRequest(DistributionChannel channel = APP) {
        BidRequest.getDefaultBidRequest(channel).tap {
            imp.first().ext.prebid.bidder.generic = null
            imp.first().ext.prebid.bidder.rubicon = Rubicon.default
        }
    }
}
