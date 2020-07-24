package org.prebid.server.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.Deal;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Pmp;
import com.iab.openrtb.response.Bid;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.request.ExtDeal;
import org.prebid.server.proto.openrtb.ext.request.ExtDealLine;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class LineItemUtil {

    private static final Logger logger = LoggerFactory.getLogger(LineItemUtil.class);

    private LineItemUtil() {
    }

    /**
     * Extracts line item ID from the given {@link Bid}.
     */
    public static String lineItemIdFrom(Bid bid, List<Imp> imps, JacksonMapper mapper) {
        if (StringUtils.isEmpty(bid.getDealid())) {
            return null;
        }
        final ExtDealLine extDealLine = extDealLineFrom(bid, imps, mapper);
        return extDealLine != null ? extDealLine.getLineItemId() : null;
    }

    /**
     * Extracts line item source from the given {@link Bid}.
     */
    public static String lineItemSourceFrom(Bid bid, List<Imp> imps, JacksonMapper mapper) {
        if (StringUtils.isEmpty(bid.getDealid())) {
            return null;
        }
        final ExtDealLine extDealLine = extDealLineFrom(bid, imps, mapper);
        return extDealLine != null ? extDealLine.getBidder() : null;
    }

    private static ExtDealLine extDealLineFrom(Bid bid, List<Imp> imps, JacksonMapper mapper) {
        return imps.stream()
                .filter(imp -> Objects.equals(imp.getId(), bid.getImpid())) // find imp by ID
                .map(Imp::getPmp)
                .filter(Objects::nonNull)
                .map(Pmp::getDeals)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .filter(deal -> Objects.equals(deal.getId(), bid.getDealid())) // find deal by ID
                .map(Deal::getExt)
                .filter(Objects::nonNull)
                .map((ObjectNode ext) -> dealExt(ext, mapper))
                .filter(Objects::nonNull)
                .map(ExtDeal::getLine)
                .findFirst()
                .orElse(null);
    }

    private static ExtDeal dealExt(JsonNode ext, JacksonMapper mapper) {
        try {
            return mapper.mapper().treeToValue(ext, ExtDeal.class);
        } catch (JsonProcessingException e) {
            logger.warn("Error decoding deal.ext: {0}", e, e.getMessage());
            return null;
        }
    }
}
