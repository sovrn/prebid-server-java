package org.prebid.server.functional.model.request.auction

import groovy.transform.ToString

@ToString(includeNames = true, ignoreNulls = true)
class Regs {

    Integer coppa
    RegsExt ext

    static Regs getDefaultRegs() {
        new Regs(ext: new RegsExt(gdpr: 0))
    }
}
