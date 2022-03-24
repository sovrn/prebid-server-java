package org.prebid.server.functional.testcontainers.scaffolding

import org.mockserver.model.HttpRequest
import org.prebid.server.functional.model.mock.services.rubiconanalytics.RubiconAnalyticsResponse
import org.prebid.server.functional.util.ObjectMapperWrapper
import org.testcontainers.containers.MockServerContainer

import static org.mockserver.model.HttpRequest.request
import static org.mockserver.model.JsonPathBody.jsonPath

class RubiconAnalytics extends NetworkScaffolding {

    private static final String ANALYTICS_ENDPOINT = "/event"

    RubiconAnalytics(MockServerContainer mockServerContainer, ObjectMapperWrapper mapper) {
        super(mockServerContainer, ANALYTICS_ENDPOINT, mapper)
    }

    @Override
    protected HttpRequest getRequest(String bidRequestId) {
        request().withPath(ANALYTICS_ENDPOINT)
                 .withBody(jsonPath("\$[?(@.auctions[0].requestId == '$bidRequestId')]"))
    }

    @Override
    protected HttpRequest getRequest() {
        request()
    }

    @Override
    void setResponse() {

    }

    RubiconAnalyticsResponse getAnalyticsRequest(String bidRequestId) {
        def analyticsRequests = getBidderRequests(bidRequestId)
        def analyticsCallCount = analyticsRequests.size()

        if (analyticsCallCount != 1) {
            throw new IllegalStateException("Expecting exactly 1 rubicon analytics call. Got $analyticsCallCount")
        }

        analyticsRequests.first()
    }

    List<RubiconAnalyticsResponse> getBidderRequests(String bidRequestId) {
        getRecordedRequestsBody(bidRequestId).collect { mapper.decode(it, RubiconAnalyticsResponse) }
    }
}
