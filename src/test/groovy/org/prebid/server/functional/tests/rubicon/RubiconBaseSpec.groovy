package org.prebid.server.functional.tests.rubicon

import org.prebid.server.functional.service.PrebidServerService
import org.prebid.server.functional.testcontainers.PBSTest
import org.prebid.server.functional.tests.BaseSpec
import org.prebid.server.functional.util.PBSUtils

import static org.prebid.server.functional.testcontainers.Dependencies.networkServiceContainer

@PBSTest
abstract class RubiconBaseSpec extends BaseSpec {

    private static final String RUBICON_VENDOR_ID = "52"
    protected static final Map<String, String> RUBICON_CONFIG =
            ["gdpr.rubicon.rsid-cookie-encryption-key" : PBSUtils.randomString,
             "gdpr.rubicon.audit-cookie-encryption-key": PBSUtils.randomString,
             "gdpr.host-vendor-id"                     : RUBICON_VENDOR_ID,
             "auction.enforce-random-bid-id"           : "false",
             "host-cookie.domain"                      : PBSUtils.randomString,
             "host-cookie.ttl-days"                    : "60",
             "adapters.rubicon.enabled"                : "true",
             "adapters.rubicon.endpoint"               : "$networkServiceContainer.rootUri/auction" as String]

    protected static final PrebidServerService rubiconPbsService = pbsServiceFactory.getService(RUBICON_CONFIG)
}
