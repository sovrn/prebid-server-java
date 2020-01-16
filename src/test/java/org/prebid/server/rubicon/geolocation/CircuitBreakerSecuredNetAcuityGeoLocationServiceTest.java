package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.execution.TimeoutFactory;
import org.prebid.server.geolocation.model.GeoInfo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(VertxUnitRunner.class)
public class CircuitBreakerSecuredNetAcuityGeoLocationServiceTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    private Vertx vertx;

    private Clock clock;

    private Timeout timeout;

    @Mock
    private NetAcuityGeoLocationService netAcuityGeoLocationService;

    @Mock
    private NetAcuityServerAddressProvider netAcuityServerAddressProvider;

    private CircuitBreakerSecuredNetAcuityGeoLocationService circuitBreakerSecuredNetAcuityGeoLocationService;

    @Before
    public void setUp() throws UnknownHostException {
        given(netAcuityServerAddressProvider.getServerAddress()).willReturn(InetAddress.getByName("localhost"));

        vertx = Vertx.vertx();
        clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        timeout = new TimeoutFactory(clock).create(1000L);

        circuitBreakerSecuredNetAcuityGeoLocationService = new CircuitBreakerSecuredNetAcuityGeoLocationService(
                netAcuityGeoLocationService, netAcuityServerAddressProvider, vertx, 1, 100L, 200L, clock);
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void lookupShouldNotCallAddressProviderAndWrappedGeoLocationIfTimeoutExceeds(TestContext context) {
        // given
        timeout = new TimeoutFactory(clock).create(clock.instant().minusMillis(1500L).toEpochMilli(), 1000L);

        // when
        final Future<GeoInfo> future = doLookup(context, timeout);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isNotNull().hasMessage("Timeout has been exceeded");

        verifyZeroInteractions(netAcuityServerAddressProvider);
        verifyZeroInteractions(netAcuityGeoLocationService);
    }

    @Test
    public void lookupShouldSucceedsIfCircuitIsClosedAndWrappedGeoLocationSucceeds(TestContext context) {
        // given
        givenWrappedGeoLocationReturning(Future.succeededFuture(
                GeoInfo.builder().country("country").vendor("vendor").build()));

        // when
        final Future<GeoInfo> future = doLookup(context);

        // then
        assertThat(future.succeeded()).isTrue();
        assertThat(future.result().getCountry()).isEqualTo("country");

        verify(netAcuityGeoLocationService).lookup(any(), any(), any());
    }

    @Test
    public void lookupShouldFailsIfCircuitIsClosedButWrappedGeoLocationFails(TestContext context) {
        // given
        givenWrappedGeoLocationReturning(Future.failedFuture(new RuntimeException("exception")));

        // when
        final Future<GeoInfo> future = doLookup(context);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(RuntimeException.class).hasMessage("exception");

        verify(netAcuityGeoLocationService).lookup(any(), any(), any());
    }

    @Test
    public void lookupShouldFailsIfCircuitIsOpened(TestContext context) {
        // given
        givenWrappedGeoLocationReturning(Future.failedFuture(new RuntimeException("exception")));

        // when
        doLookup(context); // 1 call
        final Future<?> future = doLookup(context); // 2 call

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(RuntimeException.class).hasMessage("open circuit");

        verify(netAcuityGeoLocationService).lookup(any(), any(), any()); // invoked only on 1 call
    }

    @Test
    public void lookupShouldFailsIfCircuitIsHalfOpenedButWrappedGeoLocationFails(TestContext context) {
        // given
        givenWrappedGeoLocationReturning(Future.failedFuture(new RuntimeException("exception")));

        // when
        doLookup(context); // 1 call
        doLookup(context); // 2 call
        doWaitForClosingInterval(context);
        final Future<?> future = doLookup(context); // 3 call

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(RuntimeException.class).hasMessage("exception");

        verify(netAcuityGeoLocationService, times(2)).lookup(any(), any(), any()); // invoked only on 1 & 3 calls
    }

    @Test
    public void lookupShouldSucceedsIfCircuitIsHalfOpenedAndWrappedGeoLocationSucceeds(TestContext context) {
        // given
        givenWrappedGeoLocationReturning(
                Future.failedFuture(new RuntimeException("exception")),
                Future.succeededFuture(GeoInfo.builder().country("country").vendor("vendor").build()));

        // when
        doLookup(context); // 1 call
        doLookup(context); // 2 call
        doWaitForClosingInterval(context);
        final Future<GeoInfo> future = doLookup(context); // 3 call

        // then
        assertThat(future.succeeded()).isTrue();
        assertThat(future.result().getCountry()).isEqualTo("country");

        verify(netAcuityGeoLocationService, times(2)).lookup(any(), any(), any()); // invoked only on 1 & 3 calls
    }

    @Test
    public void lookupShouldFailsWithOriginalExceptionIfOpeningIntervalExceeds(TestContext context) {
        // given
        circuitBreakerSecuredNetAcuityGeoLocationService = new CircuitBreakerSecuredNetAcuityGeoLocationService(
                netAcuityGeoLocationService, netAcuityServerAddressProvider, vertx, 2, 100L, 200L, clock);

        givenWrappedGeoLocationReturning(
                Future.failedFuture(new RuntimeException("exception1")),
                Future.failedFuture(new RuntimeException("exception2")));

        // when
        final Future<?> future1 = doLookup(context); // 1 call
        doWaitForOpeningInterval(context);
        final Future<?> future2 = doLookup(context); // 2 call

        // then
        verify(netAcuityGeoLocationService, times(2)).lookup(any(), any(), any());

        assertThat(future1.failed()).isTrue();
        assertThat(future1.cause()).isInstanceOf(RuntimeException.class).hasMessage("exception1");

        assertThat(future2.failed()).isTrue();
        assertThat(future2.cause()).isInstanceOf(RuntimeException.class).hasMessage("exception2");
    }

    @Test
    public void lookupShouldReturnFailedFutureWhenAddressProviderThrowsException(TestContext context) {
        // given
        given(netAcuityServerAddressProvider.getServerAddress()).willThrow(PreBidException.class);

        // when
        final Future<GeoInfo> future = doLookup(context);

        // then
        verifyZeroInteractions(netAcuityGeoLocationService);

        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(PreBidException.class);
    }

    @Test
    public void lookupShouldRemoveServerFromActiveListOnCircuitOpened(TestContext context) {
        // given
        givenWrappedGeoLocationReturning(Future.failedFuture(new RuntimeException("exception")));

        // when
        doLookup(context);

        // then
        verify(netAcuityServerAddressProvider).removeServerAddress(any());
    }

    @SuppressWarnings("unchecked")
    private <T> void givenWrappedGeoLocationReturning(Future... results) {
        BDDMockito.BDDMyOngoingStubbing<Future<GeoInfo>> given =
                given(netAcuityGeoLocationService.lookup(any(), any(), any()));
        for (Future<T> result : results) {
            given = given.willReturn((Future<GeoInfo>) result);
        }
    }

    private Future<GeoInfo> doLookup(TestContext context) {
        return doLookup(context, timeout);
    }

    private Future<GeoInfo> doLookup(TestContext context, Timeout timeout) {
        final Async async = context.async();

        final Future<GeoInfo> future = circuitBreakerSecuredNetAcuityGeoLocationService.lookup(null, timeout);
        future.setHandler(ar -> async.complete());

        async.await();
        return future;
    }

    private void doWaitForOpeningInterval(TestContext context) {
        doWait(context, 150L);
    }

    private void doWaitForClosingInterval(TestContext context) {
        doWait(context, 250L);
    }

    private void doWait(TestContext context, long timeout) {
        final Async async = context.async();
        vertx.setTimer(timeout, id -> async.complete());
        async.await();
    }
}
