package org.prebid.server.rubicon.geolocation;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.exception.PreBidException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

public class NetAcuityServerAddressProviderTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private Vertx vertx;

    private NetAcuityServerAddressProvider addressProvider;

    private Set<String> serverNames;

    @Before
    public void setUp() {
        serverNames = new HashSet<>(asList("localhost", "google.com"));
        addressProvider = NetAcuityServerAddressProvider.create(vertx, serverNames);
    }

    @Test
    public void createShouldFailOnAtLeastOneInvalidServerAddress() {
        // given
        serverNames.add("invalid");

        // when and then
        assertThatThrownBy(() -> NetAcuityServerAddressProvider.create(vertx, serverNames))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid NetAcuity server address: invalid");
    }

    @Test
    public void createShouldFailOnEmptyServerAddresses() {
        assertThatThrownBy(() -> NetAcuityServerAddressProvider.create(vertx, emptySet()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("No NetAcuity server addresses were specified: []");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void initializeShouldCallRefreshWithExpectedPeriod() {
        // given
        given(vertx.setPeriodic(anyLong(), any())).willAnswer(inv -> {
            ((Handler<Long>) inv.getArguments()[1]).handle(1L);
            return 0L;
        });

        final NetAcuityServerAddressProvider addressProviderSpy = Mockito.spy(this.addressProvider);

        // when
        addressProviderSpy.initialize();

        // then
        verify(vertx).setPeriodic(eq(3600000L), any());
        verify(addressProviderSpy).refresh();
    }

    @Test
    public void getServerAddressShouldReturnExpectedResult() throws UnknownHostException {
        assertThat(addressProvider.getServerAddress().getHostName())
                .isIn("localhost", "google.com");

        addressProvider.removeServerAddress(InetAddress.getByName("localhost"));

        assertThat(addressProvider.getServerAddress().getHostName())
                .isEqualTo("google.com");
    }

    @Test
    public void getServerAddressShouldThrowPrebidExceptionIfListIsEmpty() throws UnknownHostException {
        // given
        addressProvider.removeServerAddress(InetAddress.getByName("localhost"));
        addressProvider.removeServerAddress(InetAddress.getByName("google.com"));

        // when and then
        assertThatThrownBy(() -> addressProvider.getServerAddress())
                .isInstanceOf(PreBidException.class)
                .hasMessage("No NetAcuity server address available");
    }
}
