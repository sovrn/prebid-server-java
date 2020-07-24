package org.prebid.server.events;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.proto.openrtb.ext.response.Events;

import static org.assertj.core.api.Assertions.assertThat;

public class EventsServiceTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    private EventsService eventsService;

    @Before
    public void setUp() {
        eventsService = new EventsService("http://external-url");
    }

    @Test
    public void createEventsShouldReturnExpectedEvent() {
        // when
        final Events events = eventsService.createEvent("bidId", "bidder", "accountId", "lineItemId", 1000L);

        // then
        assertThat(events).isEqualTo(Events.of(
                "http://external-url/event?t=win&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&l=lineItemId",
                "http://external-url/event?t=imp&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&l=lineItemId"));
    }

    @Test
    public void createEventsShouldSkipLineItemIdIfMissing() {
        // when
        final Events events = eventsService.createEvent("bidId", "bidder", "accountId", null, 1000L);

        // then
        assertThat(events).isEqualTo(Events.of(
                "http://external-url/event?t=win&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i",
                "http://external-url/event?t=imp&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i"));
    }

    @Test
    public void winUrlTargetingShouldReturnExpectedUrl() {
        // when
        final String winUrlTargeting = eventsService.winUrlTargeting("bidder", "accountId", "lineItemId", 1000L);

        // then
        assertThat(winUrlTargeting).isEqualTo(
                "http://external-url/event?t=win&b=BIDID&a=accountId&ts=1000&bidder=bidder&f=i&l=lineItemId");
    }

    @Test
    public void winUrlTargetingShouldSkipLineItemIdIfMissing() {
        // when
        final String winUrlTargeting = eventsService.winUrlTargeting("bidder", "accountId", "lineItemId", null);

        // then
        assertThat(winUrlTargeting).isEqualTo(
                "http://external-url/event?t=win&b=BIDID&a=accountId&bidder=bidder&f=i&l=lineItemId");
    }

    @Test
    public void vastUrlShouldReturnExpectedUrl() {
        // when
        final String vastUrl = eventsService.vastUrlTracking("bidId", "bidder", "accountId", null, 1000L);

        // then
        assertThat(vastUrl).isEqualTo("http://external-url/event?t=imp&b=bidId&a=accountId&ts=1000&bidder=bidder&f=b");
    }

    @Test
    public void winUrlShouldReturnExpectedUrl() {
        // when
        final String winUrlTargeting = eventsService.vastUrlTracking("bidId", "bidder", "accountId",
                "lineItemId", 1000L);

        // then
        assertThat(winUrlTargeting).isEqualTo(
                "http://external-url/event?t=imp&b=bidId&a=accountId&ts=1000&bidder=bidder&f=b&l=lineItemId");
    }
}
