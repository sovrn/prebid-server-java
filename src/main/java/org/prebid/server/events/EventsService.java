package org.prebid.server.events;

import org.prebid.server.proto.openrtb.ext.response.Events;
import org.prebid.server.util.HttpUtil;

import java.util.Objects;

public class EventsService {

    private static final String BIDID_PLACEHOLDER = "BIDID";

    private final String externalUrl;

    public EventsService(String externalUrl) {
        this.externalUrl = HttpUtil.validateUrl(Objects.requireNonNull(externalUrl));
    }

    /**
     * Returns {@link Events} object based on given params.
     */
    public Events createEvent(String bidId, String bidder, String accountId, String lineItemId, Long timestamp) {
        return Events.of(
                eventUrl(EventRequest.Type.win, bidId, bidder, accountId, EventRequest.Format.image,
                        lineItemId, timestamp),
                eventUrl(EventRequest.Type.imp, bidId, bidder, accountId, EventRequest.Format.image,
                        lineItemId, timestamp));
    }

    /**
     * Returns value for "hb_winurl" targeting keyword.
     */
    public String winUrlTargeting(String bidder, String accountId, String lineItemId, Long timestamp) {
        return eventUrl(EventRequest.Type.win, BIDID_PLACEHOLDER, bidder, accountId,
                EventRequest.Format.image, lineItemId, timestamp);
    }

    /**
     * Returns url for win tracking.
     */
    public String winUrl(String bidId, String bidder, String accountId, Long timestamp) {
        return eventUrl(EventRequest.Type.win, bidId, bidder, accountId, EventRequest.Format.image, null, timestamp);
    }

    /**
     * Returns url for VAST tracking.
     */
    public String vastUrlTracking(String bidId, String bidder, String accountId, String lineItemId, Long timestamp) {
        return eventUrl(EventRequest.Type.imp, bidId, bidder, accountId, EventRequest.Format.blank, lineItemId,
                timestamp);
    }

    private String eventUrl(EventRequest.Type type, String bidId, String bidder, String accountId,
                            EventRequest.Format format, String lineItemId, Long timestamp) {
        final EventRequest eventRequest = EventRequest.builder()
                .type(type)
                .bidId(bidId)
                .accountId(accountId)
                .bidder(bidder)
                .timestamp(timestamp)
                .format(format)
                .lineItemId(lineItemId)
                .build();

        return EventUtil.toUrl(externalUrl, eventRequest);
    }
}
