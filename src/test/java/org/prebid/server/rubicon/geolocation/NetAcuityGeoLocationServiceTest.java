package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.execution.TimeoutFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(VertxUnitRunner.class)
public class NetAcuityGeoLocationServiceTest {

    private Vertx vertx;

    private NetAcuityGeoLocationService netAcuityGeoLocationService;

    @Before
    public void setUp() {
        vertx = Vertx.vertx();
        netAcuityGeoLocationService = NetAcuityGeoLocationService.create(vertx, "localhost");
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void createShouldFailOnAtLeastOneInvalidServerAddress() {
        assertThatThrownBy(() -> NetAcuityGeoLocationService.create(vertx, "localhost,invalid"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid NetAcuity server address: invalid");
    }

    @Test
    public void createShouldFailOnEmptyServerAddresses() {
        assertThatThrownBy(() -> NetAcuityGeoLocationService.create(vertx, ","))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("No NetAcuity server addresses was specified: ,");
    }

    @Test
    public void createShouldTolerateSpacesForServers() {
        // given
        final String server = "localhost,     localhost  ,localhost , localhost";

        // when
        final NetAcuityGeoLocationService geoLocationService = NetAcuityGeoLocationService.create(vertx, server);

        // then
        assertThat(geoLocationService).isNotNull();
    }

    @Test
    public void lookupShouldFailIfClientAddressIsInvalid() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        final Timeout timeout = timeoutFactory.create(500L);

        // when
        final Future<?> future = netAcuityGeoLocationService.lookup("invalid", timeout);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause())
                .isInstanceOf(PreBidException.class)
                .hasMessage("Invalid IP address to lookup: invalid");
    }

    @Test
    public void lookupShouldFailIfGlobalTimeoutExceeded() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        final Timeout expiredTimeout = timeoutFactory.create(clock.instant().minusMillis(1500L).toEpochMilli(), 1000L);

        // when
        final Future<?> future = netAcuityGeoLocationService.lookup(null, expiredTimeout);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(TimeoutException.class);
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
}
