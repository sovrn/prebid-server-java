package org.prebid.server.functional.model

import com.fasterxml.jackson.annotation.JsonFormat
import groovy.transform.ToString
import org.prebid.server.functional.model.bidder.BidderName
import org.prebid.server.functional.model.request.setuid.UidWithExpiry

import java.time.Clock
import java.time.ZonedDateTime

import static org.prebid.server.functional.model.bidder.BidderName.GENERIC

@ToString(includeNames = true, ignoreNulls = true)
class UidsCookie {

    Map<String, String> uids
    Map<String, UidWithExpiry> tempUIDs
    Boolean optout
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'", timezone = "UTC")
    ZonedDateTime bday

    static UidsCookie getDefaultUidsCookie(BidderName bidderName = GENERIC) {
        def uidsCookie = new UidsCookie()
        uidsCookie.uids = [(bidderName.value): UUID.randomUUID().toString()]
        uidsCookie.bday = ZonedDateTime.now(Clock.systemUTC())
        uidsCookie.tempUIDs = [(bidderName.value): new UidWithExpiry(uid: UUID.randomUUID().toString(), expires: ZonedDateTime.now(Clock.systemUTC()).plusDays(2))]
        uidsCookie
    }
}
