package org.prebid.server.rubicon.analytics.proto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Builder
@Value
public class Event {

    @JsonProperty("eventTimeMillis")
    Long eventTimeMillis;

    String integration;

    @JsonProperty("wrapperName")
    String wrapperName;

    String version;

    Client client;

    @JsonProperty("limitAdTracking")
    Boolean limitAdTracking;

    List<Auction> auctions;

    @JsonProperty("bidsWon")
    List<BidWon> bidsWon;

    @JsonProperty("eventCreator")
    EventCreator eventCreator;

    List<Impression> impressions;

    @JsonProperty("userAgent")
    String userAgent;

    @JsonProperty("referrerUri")
    String referrerUri;

    String country;
}
