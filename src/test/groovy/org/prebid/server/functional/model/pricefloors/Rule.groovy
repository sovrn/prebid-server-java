package org.prebid.server.functional.model.pricefloors

import com.fasterxml.jackson.annotation.JsonValue
import org.apache.commons.lang3.StringUtils

import java.lang.reflect.Modifier

class Rule {

    private static final String DEFAULT_DELIMITER = "|"

    private String delimiter
    private String siteDomain
    private String pubDomain
    private String domain
    private String bundle
    private String channel
    private MediaType mediaType
    private String size
    private String gptSlot
    private String pbAdSlot
    private Country country
    private DeviceType deviceType

    @JsonValue
    String getRule() {
        defineDelimiter()
        def stringBuilder = new StringBuilder()
        this.class.declaredFields.findAll { !it.synthetic && !Modifier.isStatic(it.modifiers) && this[it.name] != null && it.name != "delimiter" }.each {
            stringBuilder << this[it.name] << delimiter
        }
        StringUtils.removeEnd(stringBuilder.toString(), delimiter).toLowerCase()
    }

    @JsonValue
    String getRule(List<PriceFloorField> fields) {
        defineDelimiter()
        def stringBuilder = new StringBuilder()
        def classFields = this.class.declaredFields.findAll { !it.synthetic }
        fields.each { field ->
            def classField = classFields.find { it.name == field.value }?.name
            stringBuilder << this[classField] << delimiter
        }
        StringUtils.removeEnd(stringBuilder.toString(), delimiter).toLowerCase()
    }

    private void defineDelimiter() {
        if (!delimiter) {
            delimiter = DEFAULT_DELIMITER
        }
    }
}
