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
        final Events events = eventsService.createEvent("bidId", "bidder", "accountId", "lineItemId", true, 1000L,
                "pbjs");

        // then
        assertThat(events).isEqualTo(Events.of(
                "http://external-url/event?t=win&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&int=pbjs&l=lineItemId",
                "http://external-url/event?t=imp&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&int=pbjs&l=lineItemId"));
    }

    @Test
    public void createEventsShouldSkipLineItemIdIfMissing() {
        // when
        final Events events = eventsService.createEvent("bidId", "bidder", "accountId", null, true, 1000L, "pbjs");

        // then
        assertThat(events).isEqualTo(Events.of(
                "http://external-url/event?t=win&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&int=pbjs",
                "http://external-url/event?t=imp&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&int=pbjs"));
    }

    @Test
    public void createEventsShouldSetAnalyticsDisabled() {
        // when
        final Events events = eventsService.createEvent("bidId", "bidder", "accountId", "lineItemId", false, 1000L,
                "pbjs");

        // then
        assertThat(events).isEqualTo(Events.of(
                "http://external-url/event?t=win&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&int=pbjs&x=0"
                        + "&l=lineItemId",
                "http://external-url/event?t=imp&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&int=pbjs&x=0"
                        + "&l=lineItemId"));
    }

    @Test
    public void winUrlShouldReturnExpectedUrl() {
        // when
        final String winUrl = eventsService.winUrl("bidId", "bidder", "accountId", "lineItemId", true, 1000L, "pbjs");

        // then
        assertThat(winUrl).isEqualTo(
                "http://external-url/event?t=win&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&int=pbjs&l=lineItemId");
    }

    @Test
    public void winUrlShouldSEtAnalyticsDisabled() {
        // when
        final String winUrl = eventsService.winUrl("bidId", "bidder", "accountId", "lineItemId", false, 1000L, "pbjs");

        // then
        assertThat(winUrl).isEqualTo(
                "http://external-url/event?t=win&b=bidId&a=accountId&ts=1000&bidder=bidder&f=i&int=pbjs&x=0"
                        + "&l=lineItemId");
    }

    @Test
    public void vastUrlShouldReturnExpectedUrl() {
        // when
        final String vastUrl = eventsService.vastUrlTracking(
                "bidId", "bidder", "accountId", "lineItemId", 1000L, "pbjs");

        // then
        assertThat(vastUrl).isEqualTo(
                "http://external-url/event?t=imp&b=bidId&a=accountId&ts=1000&bidder=bidder&f=b&int=pbjs&l=lineItemId");
    }
}
