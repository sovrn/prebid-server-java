package org.prebid.server.functional.tests.rubicon

import com.iabtcf.encoder.TCStringEncoder
import com.iabtcf.utils.BitSetIntIterable
import org.prebid.server.functional.model.UidsCookie
import org.prebid.server.functional.model.request.setuid.SetuidRequest
import org.prebid.server.functional.service.PrebidServerService
import org.prebid.server.functional.util.PBSUtils
import spock.lang.Shared

import java.util.concurrent.TimeUnit

import static org.prebid.server.functional.model.bidder.BidderName.RUBICON

class SetuidSpec extends RubiconBaseSpec {

    //TODO: update the way the audit cookie is generated
    private static final String AUDIT_COOKIE = "GzhqHDTACWuj-m1FXJic6E0Vp8brrzoQDFmowPV1LQu3B5VuyOoHmwlrKPG-yV6dWro" +
            "EWamQVf4xDK4wvz3qfXH7yqO4Ns-k9Vdvwr2-X9phOTmuFgzpjw== "
    private static final String HOST_COOKIE_DOMAIN = PBSUtils.randomString
    private static final Integer HOST_COOKIE_TTL_DAYS = 60

    @Shared
    PrebidServerService prebidServerService = pbsServiceFactory.getService(RUBICON_CONFIG <<
            ["host-cookie.domain"  : HOST_COOKIE_DOMAIN,
             "host-cookie.ttl-days": HOST_COOKIE_TTL_DAYS as String])

    def "PBS should return cookies when request contains uids and uids-audit cookies"() {
        given: "SetuidRequest with rubicon"
        //TODO: update the way the consent string is generated after the merge PR
        def consentString = TCStringEncoder.newBuilder()
                                           .version(2)
                                           .consentLanguage("EN")
                                           .vendorListVersion(52)
                                           .tcfPolicyVersion(2)
                                           .addPurposesConsent(BitSetIntIterable.from(1))
                                           .addVendorConsent(BitSetIntIterable.from(52))
                                           .encode()
        def request = SetuidRequest.rubiconSetuidRequest.tap {
            gdpr = 1
            gdprConsent = consentString
            account = PBSUtils.randomNumber
            uid = PBSUtils.randomNumber
        }

        and: "Uids and uids-audit request cookies"
        def uidsCookie = UidsCookie.getDefaultUidsCookie(RUBICON)

        when: "PBS processes setuid request"
        def response = prebidServerService.sendSetUidRequest(request, uidsCookie, AUDIT_COOKIE)

        then: "Response should contain uids, uids-audit cookie"
        def cookieHeader = response.headers["Set-Cookie"]
        assert cookieHeader
        assert cookieHeader.size() == 4

        def cookies = cookieHeader.collect { parseCookie(it) }

        def expiredUidCookie = cookies.find { it["uids"] && it["uids"] == "deleted" }
        def newUidCookie = cookies.find { it["uids"] && it["uids"] != "deleted" }
        def expiredUidAuditCookie = cookies.find { it["uids-audit"] && it["uids-audit"] == "deleted" }
        def newUidAuditCookie = cookies.find { it["uids-audit"] && it["uids-audit"] != "deleted" }

        verifyAll {
            assert expiredUidCookie["Max-Age"] == "0"
            assert expiredUidCookie["Domain"] == "rubiconproject.com"

            assert newUidCookie["Max-Age"] == TimeUnit.DAYS.toSeconds(HOST_COOKIE_TTL_DAYS) as String
            assert newUidCookie["Domain"] == HOST_COOKIE_DOMAIN

            assert expiredUidAuditCookie["Max-Age"] == "0"
            assert expiredUidAuditCookie["Domain"] == "rubiconproject.com"

            assert newUidAuditCookie["Max-Age"] == TimeUnit.DAYS.toSeconds(HOST_COOKIE_TTL_DAYS) as String
            assert newUidAuditCookie["Domain"] == HOST_COOKIE_DOMAIN
        }
    }

    def "PBS should return cookies when request doesn't contain uids and uids-audit cookies"() {
        given: "SetuidRequest with rubicon"
        //TODO: update the way the consent string is generated after the merge PR
        def consentString = TCStringEncoder.newBuilder()
                                           .version(2)
                                           .consentLanguage("EN")
                                           .vendorListVersion(52)
                                           .tcfPolicyVersion(2)
                                           .addPurposesConsent(BitSetIntIterable.from(1))
                                           .addVendorConsent(BitSetIntIterable.from(52))
                                           .encode()
        def request = SetuidRequest.rubiconSetuidRequest.tap {
            gdpr = 1
            gdprConsent = consentString
            account = PBSUtils.randomNumber
            uid = PBSUtils.randomNumber
        }

        when: "PBS processes setuid request"
        def response = prebidServerService.sendSetUidRequest(request)

        then: "Response should contain uids, uids-audit cookie"
        def cookieHeader = response.headers["Set-Cookie"]
        assert cookieHeader
        assert cookieHeader.size() == 4

        def cookies = cookieHeader.collect { parseCookie(it) }

        def expiredUidCookie = cookies.find { it["uids"] && it["uids"] == "deleted" }
        def newUidCookie = cookies.find { it["uids"] && it["uids"] != "deleted" }
        def expiredUidAuditCookie = cookies.find { it["uids-audit"] && it["uids-audit"] == "deleted" }
        def newUidAuditCookie = cookies.find { it["uids-audit"] && it["uids-audit"] != "deleted" }

        verifyAll {
            assert expiredUidCookie["Max-Age"] == "0"
            assert expiredUidCookie["Domain"] == "rubiconproject.com"

            assert newUidCookie["Max-Age"] == TimeUnit.DAYS.toSeconds(HOST_COOKIE_TTL_DAYS) as String
            assert newUidCookie["Domain"] == HOST_COOKIE_DOMAIN

            assert expiredUidAuditCookie["Max-Age"] == "0"
            assert expiredUidAuditCookie["Domain"] == "rubiconproject.com"

            assert newUidAuditCookie["Max-Age"] == TimeUnit.DAYS.toSeconds(HOST_COOKIE_TTL_DAYS) as String
            assert newUidAuditCookie["Domain"] == HOST_COOKIE_DOMAIN
        }
    }

    private static Map<String, String> parseCookie(String cookie) {
        cookie.split("; ").collectEntries {
            if (it.contains("=")) {
                def keyValueString = it.split("=")
                keyValueString.size() == 2 ? [keyValueString[0], keyValueString[1]] : [:]
            } else {
                [it, ""]
            }
        }
    }
}
