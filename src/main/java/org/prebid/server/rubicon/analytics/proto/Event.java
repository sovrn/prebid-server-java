package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Builder
@Value
public class Event {

    String integration;

    String version;

    Client client;

    @JsonProperty("limitAdTracking")
    Boolean limitAdTracking;

    List<Auction> auctions;

    @JsonProperty("bidsWon")
    List<BidWon> bidsWon;

    @JsonProperty("eventCreator")
    EventCreator eventCreator;

    @JsonProperty("userAgent")
    String userAgent;

    String country;
}
