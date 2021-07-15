package org.prebid.server.rubicon.rsid;

import io.vertx.core.http.Cookie;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.model.CaseInsensitiveMultiMap;
import org.prebid.server.model.HttpRequestContext;
import org.prebid.server.rubicon.rsid.model.Rsid;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

public class RsidCookieServiceTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    private RsidCookieService rsidCookieService;

    @Mock
    private RoutingContext routingContext;

    @Before
    public void setUp() {
        rsidCookieService = new RsidCookieService("fdvndnvavad jdavao89571-34242134fdfads");
    }

    @Test
    public void shouldReturnNullIfNoCookieInRequest() {
        // given
        given(routingContext.cookieMap()).willReturn(emptyMap());

        // when
        final Rsid rsid = rsidCookieService.parseFromRequest(routingContext);

        // then
        assertThat(rsid).isNull();
    }

    @Test
    public void shouldReturnNullIfCookieValueIsInvalid() {
        // given
        given(routingContext.cookieMap())
                .willReturn(singletonMap("rsid", Cookie.cookie("rsid", "invalid-base64")));

        // when
        final Rsid rsid = rsidCookieService.parseFromRequest(routingContext);

        // then
        assertThat(rsid).isNull();
    }

    @Test
    public void shouldCutOutPipeAndAllCharactersBeforeItAndReturnExpectedResult() {
        // given
        given(routingContext.cookieMap())
                .willReturn(singletonMap("rsid", Cookie.cookie("rsid",
                        "chars_before_pipe|B9qWECXyvoJUFeX6MlUI0rdsb6KO+1hVre/oD1mN/CN4VoLIUnj4T/IHduc/n6k03b"
                                + "YgvBh7oB3JHIxCI7JZAa8E5oMBeRSWa9qr15frXLoJaNEy0hbrXDlIwC9iqGWqIrmhaA==")));

        // when
        final Rsid rsid = rsidCookieService.parseFromRequest(routingContext);

        // then
        assertThat(rsid).isEqualTo(Rsid.of("us"));
    }

    @Test
    public void shouldReturnExpectedResultsForVarianceOfInputs() {
        // given
        given(routingContext.cookieMap())
                .willReturn(singletonMap("rsid", Cookie.cookie("rsid",
                        "B9qWECXyvoJUFeX6MlUI0rdsb6KO+1hVre/oD1mN/CN4VoLIUnj4T/IHduc/n6k03bYgvBh7oB3JHI"
                                + "xCI7JZAa8E5oMBeRSWa9qr15frXLoJaNEy0hbrXDlIwC9iqGWqIrmhaA==")))
                .willReturn(singletonMap("rsid", Cookie.cookie("rsid",
                        "EsePVWzrq5VIHvKucUcPzKQlZamJwB9lpvqWFkawwXF4ANOheTKUHc1geuA89cYw39hHoR9/pR3Kc4x"
                                + "Ca+Q3UpASoq5sdBSLYfe1+4q/He9gbrxi/lS7CmkHnH0JqQiyRfnlPKNzCa5H/06D7G2+pKm4A/zGhhk=")))
                .willReturn(singletonMap("rsid", Cookie.cookie("rsid",
                        "EsuOVTz9qY5AEuOucUkAzbRrZaSc0QRkrf3oSXS9/hxTHeycbR/yTp9gc+E89c4x3LRLzxx6oB3Ueow"
                                + "sGO40PP909p4BND+MZsqo59SuHehbMpYu71S3MA==")));

        // when
        final List<Rsid> rsids = Arrays.asList(
                rsidCookieService.parseFromRequest(routingContext),
                rsidCookieService.parseFromRequest(routingContext),
                rsidCookieService.parseFromRequest(routingContext));

        // then
        final Rsid expected = Rsid.of("us");
        assertThat(rsids).containsOnly(expected, expected, expected);
    }

    @Test
    public void parseFromRequestShouldReturnExpectedResult() {
        // given
        final CaseInsensitiveMultiMap headers = CaseInsensitiveMultiMap.builder()
                .add(
                        HttpHeaders.COOKIE,
                        Cookie.cookie(
                                "rsid",
                                "B9qWECXyvoJUFeX6MlUI0rdsb6KO+1hVre/oD1mN/CN4VoLIUnj4T/IHduc/n6k03bYgvBh7oB3JHI"
                                        + "xCI7JZAa8E5oMBeRSWa9qr15frXLoJaNEy0hbrXDlIwC9iqGWqIrmhaA==")
                                .encode())
                .build();
        final HttpRequestContext httpRequest = HttpRequestContext.builder().headers(headers).build();

        // when
        final Rsid rsid = rsidCookieService.parseFromRequest(httpRequest);

        // then
        assertThat(rsid).isEqualTo(Rsid.of("us"));
    }
}
