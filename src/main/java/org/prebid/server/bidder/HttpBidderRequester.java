package org.prebid.server.bidder;

import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.response.Bid;
import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.prebid.server.auction.ExchangeService;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.BidderSeatBid;
import org.prebid.server.bidder.model.HttpCall;
import org.prebid.server.bidder.model.HttpRequest;
import org.prebid.server.bidder.model.HttpResponse;
import org.prebid.server.bidder.model.Result;
import org.prebid.server.execution.Timeout;
import org.prebid.server.proto.openrtb.ext.response.ExtHttpCall;
import org.prebid.server.vertx.http.HttpClient;
import org.prebid.server.vertx.http.model.HttpClientResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Implements HTTP communication functionality common for {@link Bidder}'s.
 * <p>
 * This class exists to help segregate core auction logic and minimize code duplication across the {@link Bidder}
 * implementations.
 * <p>
 * Any logic which can be done within a single Seat goes inside this class.
 * Any logic which requires responses from all Seats goes inside the {@link ExchangeService}.
 */
public class HttpBidderRequester {

    private static final Logger logger = LoggerFactory.getLogger(HttpBidderRequester.class);

    private final HttpClient httpClient;

    public HttpBidderRequester(HttpClient httpClient) {
        this.httpClient = Objects.requireNonNull(httpClient);
    }

    /**
     * Executes given request to a given bidder.
     */
    public <T> Future<BidderSeatBid> requestBids(Bidder<T> bidder, BidRequest bidRequest, Timeout timeout,
                                                 boolean debugEnabled) {
        final Result<List<HttpRequest<T>>> httpRequestsWithErrors = bidder.makeHttpRequests(bidRequest);

        final List<BidderError> bidderErrors = httpRequestsWithErrors.getErrors();
        final List<HttpRequest<T>> httpRequests = httpRequestsWithErrors.getValue();

        return CollectionUtils.isEmpty(httpRequests)
                ? emptyBidderSeatBidWithErrors(bidderErrors)
                : CompositeFuture.join(httpRequests.stream()
                .map(httpRequest -> doRequest(httpRequest, timeout))
                .collect(Collectors.toList()))
                .map(httpRequestsResult ->
                        toBidderSeatBid(bidder, bidRequest, httpRequestsResult.list(), debugEnabled, bidderErrors));
    }

    /**
     * Creates {@link Future<BidderSeatBid>} with empty list of {@link BidderBid}s
     * and list of {@link ExtHttpCall}s with list of {@link BidderError}s.
     * If errors list is empty, creates error which indicates of bidder unexpected behaviour.
     */
    private Future<BidderSeatBid> emptyBidderSeatBidWithErrors(List<BidderError> bidderErrors) {
        return Future.succeededFuture(
                BidderSeatBid.of(Collections.emptyList(), Collections.emptyList(), bidderErrors.isEmpty()
                        ? Collections.singletonList(BidderError.failedToRequestBids(
                        "The bidder failed to generate any bid requests, but also failed to generate an error"))
                        : bidderErrors));
    }

    /**
     * Makes an HTTP request and returns {@link Future} that will be eventually completed with success or error result.
     */
    private <T> Future<HttpCall<T>> doRequest(HttpRequest<T> httpRequest, Timeout timeout) {
        final long remainingTimeout = timeout.remaining();
        if (remainingTimeout <= 0) {
            return failResponse(new TimeoutException("Timeout has been exceeded"), httpRequest);
        }

        return httpClient.request(httpRequest.getMethod(), httpRequest.getUri(), httpRequest.getHeaders(),
                httpRequest.getBody(), remainingTimeout)
                .compose(response -> processResponse(response, httpRequest))
                .recover(exception -> failResponse(exception, httpRequest));
    }

    /**
     * Produces {@link Future} with {@link HttpCall} containing request and error description.
     */
    private static <T> Future<HttpCall<T>> failResponse(Throwable exception, HttpRequest<T> httpRequest) {
        logger.warn("Error occurred while sending HTTP request to a bidder url: {0}", exception, httpRequest.getUri());
        final BidderError.Type errorType =
                exception instanceof TimeoutException || exception instanceof ConnectTimeoutException
                        ? BidderError.Type.timeout
                        : BidderError.Type.generic;

        return Future.succeededFuture(
                HttpCall.failure(httpRequest, BidderError.create(exception.getMessage(), errorType)));
    }

    /**
     * Produces {@link Future} with {@link HttpCall} containing request, response and possible error description
     * (if status code indicates an error).
     */
    private static <T> Future<HttpCall<T>> processResponse(HttpClientResponse response, HttpRequest<T> httpRequest) {
        final int statusCode = response.getStatusCode();
        return Future.succeededFuture(HttpCall.success(httpRequest,
                HttpResponse.of(statusCode, response.getHeaders(), response.getBody()), errorOrNull(statusCode)));
    }

    /**
     * Returns {@link BidderError} if HTTP status code is not successful, or null otherwise.
     */
    private static BidderError errorOrNull(int statusCode) {
        if (statusCode != HttpResponseStatus.OK.code() && statusCode != HttpResponseStatus.NO_CONTENT.code()) {
            return BidderError.create(String.format(
                    "Unexpected status code: %d. Run with request.test = 1 for more info", statusCode),
                    statusCode == HttpResponseStatus.BAD_REQUEST.code()
                            ? BidderError.Type.bad_input
                            : BidderError.Type.bad_server_response);
        }
        return null;
    }

    /**
     * Transforms HTTP call results into single {@link BidderSeatBid} filled with debug information, bids and errors
     * happened along the way.
     */
    private <T> BidderSeatBid toBidderSeatBid(Bidder<T> bidder, BidRequest bidRequest, List<HttpCall<T>> calls,
                                              boolean debugEnabled, List<BidderError> previousErrors) {
        // Capture debugging info from the requests
        final List<ExtHttpCall> httpCalls = debugEnabled
                ? calls.stream().map(HttpBidderRequester::toExt).collect(Collectors.toList())
                : Collections.emptyList();

        final List<Result<List<BidderBid>>> createdBids = calls.stream()
                .filter(httpCall -> httpCall.getError() == null)
                .filter(HttpBidderRequester::isOkOrNoContent)
                .map(httpCall -> bidder.makeBids(httpCall, bidRequest))
                .collect(Collectors.toList());

        final List<BidderBid> bids = createdBids.stream()
                .flatMap(bidderBid -> bidderBid.getValue().stream())
                .collect(Collectors.toList());

        final List<BidderError> bidderErrors = errors(previousErrors, calls, createdBids, bidRequest);

        return BidderSeatBid.of(bids, httpCalls, bidderErrors);
    }

    /**
     * Constructs {@link ExtHttpCall} filled with HTTP call information.
     */
    private static <T> ExtHttpCall toExt(HttpCall<T> httpCall) {
        final HttpRequest<T> request = httpCall.getRequest();
        final ExtHttpCall.ExtHttpCallBuilder builder = ExtHttpCall.builder()
                .uri(request.getUri())
                .requestbody(request.getBody());

        final HttpResponse response = httpCall.getResponse();
        if (response != null) {
            builder.responsebody(response.getBody());
            builder.status(response.getStatusCode());
        }

        return builder.build();
    }

    private static boolean isOkOrNoContent(HttpCall httpCall) {
        final int statusCode = httpCall.getResponse().getStatusCode();
        return statusCode == HttpResponseStatus.OK.code() || statusCode == HttpResponseStatus.NO_CONTENT.code();
    }

    /**
     * Assembles all errors for {@link BidderSeatBid} into the list of {@link List}&lt;{@link BidderError}&gt;
     */
    private static <R> List<BidderError> errors(List<BidderError> requestErrors, List<HttpCall<R>> calls,
                                                List<Result<List<BidderBid>>> createdBids,
                                                BidRequest bidRequest) {

        final Set<String> impIdsFromPrebidRequest = impIdsFromPrebidRequest(bidRequest);
        final Set<String> impIdsFromExchangeRequests = impIdsFromExchangeRequests(calls);
        final Set<String> requestErrorImpIds = subtract(impIdsFromPrebidRequest, impIdsFromExchangeRequests);
        final List<BidderError> requestErrorsWithImpIds = populateImpIds(requestErrorImpIds, requestErrors);

        final Set<String> httpErrorImpIds = httpErrorImpIds(calls);
        final List<BidderError> httpErrors = httpErrors(calls);
        final List<BidderError> httpErrorsWithImpIds = populateImpIds(httpErrorImpIds, httpErrors);

        final Set<String> impIdsWithoutHttpErrors = subtract(impIdsFromExchangeRequests, httpErrorImpIds);
        final Set<String> impIdsFromResponse = impIdsFromResponse(createdBids);
        final Set<String> responseErrorImpIds = subtract(impIdsWithoutHttpErrors, impIdsFromResponse);
        final List<BidderError> responseErrors = responseErrors(createdBids);
        final List<BidderError> responseErrorsWithImpIds = populateImpIds(responseErrorImpIds, responseErrors);

        final List<BidderError> bidderErrors = new ArrayList<>(requestErrorsWithImpIds);
        bidderErrors.addAll(httpErrorsWithImpIds);
        bidderErrors.addAll(responseErrorsWithImpIds);
        return bidderErrors;
    }

    private static Set<String> impIdsFromPrebidRequest(BidRequest bidRequest) {
        return bidRequest.getImp().stream()
                .map(Imp::getId)
                .collect(Collectors.toSet());
    }

    private static <R> Set<String> impIdsFromExchangeRequests(List<HttpCall<R>> calls) {
        return calls.stream()
                .map(call -> call.getRequest().getPayload())
                .filter(BidRequest.class::isInstance)
                .map(BidRequest.class::cast)
                .flatMap(request -> request.getImp().stream())
                .map(Imp::getId)
                .collect(Collectors.toSet());
    }

    private static Set<String> impIdsFromResponse(List<Result<List<BidderBid>>> createdBids) {
        return createdBids.stream()
                .flatMap(listResult -> listResult.getValue().stream())
                .map(BidderBid::getBid)
                .filter(Objects::nonNull)
                .map(Bid::getImpid)
                .collect(Collectors.toSet());
    }

    private static <R> Set<String> httpErrorImpIds(List<HttpCall<R>> calls) {
        return calls.stream()
                .filter(call -> call.getError() != null)
                .map(call -> call.getRequest().getPayload())
                .filter(BidRequest.class::isInstance)
                .map(BidRequest.class::cast)
                .flatMap(request -> request.getImp().stream())
                .map(Imp::getId)
                .collect(Collectors.toSet());
    }

    private static <R> List<BidderError> httpErrors(List<HttpCall<R>> calls) {
        return calls.stream()
                .map(HttpCall::getError)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static List<BidderError> responseErrors(List<Result<List<BidderBid>>> createdBids) {
        return createdBids.stream()
                .flatMap(bidResult -> bidResult.getErrors().stream())
                .collect(Collectors.toList());
    }

    private static List<BidderError> populateImpIds(Set<String> impIds, List<BidderError> errors) {
        return errors.stream()
                .map(error -> BidderError.of(error.getMessage(), error.getType(),
                        CollectionUtils.isNotEmpty(impIds) ? impIds : null))
                .collect(Collectors.toList());
    }

    private static Set<String> subtract(Set<String> first, Set<String> second) {
        final Set<String> result = new HashSet<>(first);
        result.removeAll(second);
        return result;
    }
}
