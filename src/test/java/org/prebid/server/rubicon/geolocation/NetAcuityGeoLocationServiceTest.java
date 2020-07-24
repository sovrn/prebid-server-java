package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.execution.TimeoutFactory;
import org.prebid.server.geolocation.model.GeoInfo;
import org.prebid.server.metric.Metrics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@RunWith(VertxUnitRunner.class)
public class NetAcuityGeoLocationServiceTest {

    @Rule
    public final MockitoRule rule = MockitoJUnit.rule();

    private Vertx vertx;

    private NetAcuityGeoLocationService netAcuityGeoLocationService;

    @Mock
    private NetAcuityServerAddressProvider addressProvider;

    @Mock
    private Clock clock;

    @Mock
    private Metrics metrics;

    @Before
    public void setUp() throws UnknownHostException {
        vertx = Vertx.vertx();
        netAcuityGeoLocationService = new NetAcuityGeoLocationService(vertx, addressProvider::getServerAddress,
                clock, metrics);

        given(addressProvider.getServerAddress()).willReturn(InetAddress.getByName("localhost"));
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void lookupShouldFailIfClientAddressIsInvalid(TestContext context) {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        final Timeout timeout = timeoutFactory.create(500L);

        // when
        final Future<?> future = netAcuityGeoLocationService.lookup("invalid", timeout);

        // then
        future.setHandler(context.asyncAssertFailure(throwable -> assertThat(throwable)
                .isInstanceOf(PreBidException.class)
                .hasMessage("Invalid IP address to lookup: invalid")));
    }

    @Test
    public void lookupShouldFailIfGlobalTimeoutExceeded(TestContext context) {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        final Timeout expiredTimeout = timeoutFactory.create(clock.instant().minusMillis(1500L).toEpochMilli(), 1000L);

        // when
        final Future<?> future = netAcuityGeoLocationService.lookup(null, expiredTimeout);

        // then
        future.setHandler(context.asyncAssertFailure(throwable ->
                assertThat(throwable).isInstanceOf(TimeoutException.class).hasMessage("Timeout has been exceeded")));
    }

    @Test
    public void lookupShouldFailIfGeoLookupRequestTimedOut(TestContext context) {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        Timeout timeout = timeoutFactory.create(1L);

        // when
        final Future<?> future = netAcuityGeoLocationService.lookup("localhost", timeout);

        // then
        future.setHandler(context.asyncAssertFailure(throwable ->
                assertThat(throwable).isInstanceOf(PreBidException.class).hasMessage("Geo location lookup failed")));
    }

    @Test
    public void lookupShouldReturnFailedFutureWhenAddressProviderThrowsException() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        Timeout timeout = timeoutFactory.create(500L);

        given(addressProvider.getServerAddress()).willThrow(PreBidException.class);

        // when
        final Future<GeoInfo> future = netAcuityGeoLocationService.lookup("localhost", timeout);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(PreBidException.class);
    }
}
