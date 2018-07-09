package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Future;
import org.junit.Before;
import org.junit.Test;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.execution.TimeoutFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NetAcuityGeoLocationServiceTest {

    private NetAcuityGeoLocationService netAcuityGeoLocationService;

    @Before
    public void setUp() {
        netAcuityGeoLocationService = NetAcuityGeoLocationService.create("localhost");
    }

    @Test
    public void createShouldFailOnInvalidServerAddress() {
        assertThatThrownBy(() -> NetAcuityGeoLocationService.create("invalid"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid NetAcuity server address: invalid");
    }

    @Test
    public void lookupShouldFailIfClientAddressIsInvalid() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        Timeout timeout = timeoutFactory.create(500L);

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
        Timeout expiredTimeout = timeoutFactory.create(clock.instant().minusMillis(1500L).toEpochMilli(), 1000L);

        // when
        final Future<?> future = netAcuityGeoLocationService.lookup(null, expiredTimeout);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause()).isInstanceOf(TimeoutException.class);
    }

    @Test
    public void lookupShouldFailIfGeoLookupRequestTimedOut() {
        // given
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final TimeoutFactory timeoutFactory = new TimeoutFactory(clock);
        Timeout timeout = timeoutFactory.create(1L);

        // when
        final Future<?> future = netAcuityGeoLocationService.lookup("localhost", timeout);

        // then
        assertThat(future.failed()).isTrue();
        assertThat(future.cause())
                .isInstanceOf(PreBidException.class)
                .hasMessage("Geo location lookup failed: Receive timed out");
    }
}
