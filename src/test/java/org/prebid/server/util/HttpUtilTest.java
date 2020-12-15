package org.prebid.server.util;

import io.vertx.core.MultiMap;
import io.vertx.core.http.Cookie;
import io.vertx.ext.web.RoutingContext;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.exception.PreBidException;

import java.time.ZonedDateTime;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.BDDMockito.given;

public class HttpUtilTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private RoutingContext routingContext;

    @Test
    public void validateUrlShouldFailOnInvalidUrl() {
        // when and then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> HttpUtil.validateUrl("invalid_url"))
                .isInstanceOf(IllegalArgumentException.class)
                .withMessage("URL supplied is not valid: invalid_url");
    }

    @Test
    public void validateUrlShouldReturnExpectedUrl() {
        // when
        final String url = HttpUtil.validateUrl("http://domain.org/query-string?a=1");

        // then
        assertThat(url).isNotNull();
        assertThat(url).isEqualTo("http://domain.org/query-string?a=1");
    }

    @Test
    public void encodeUrlShouldReturnExpectedValue() {
        // when
        final String url = HttpUtil.encodeUrl("//domain.org/query-string?a=1");

        // then
        assertThat(url).isNotNull();
        assertThat(url).isEqualTo("%2F%2Fdomain.org%2Fquery-string%3Fa%3D1");
    }

    @Test
    public void addHeaderIfValueIsNotEmptyShouldAddHeaderIfValueIsNotEmptyAndNotNull() {
        // given
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap();

        // when
        HttpUtil.addHeaderIfValueIsNotEmpty(headers, "header", "value");

        // then
        assertThat(headers)
                .extracting(Map.Entry::getKey, Map.Entry::getValue)
                .containsOnly(tuple("header", "value"));
    }

    @Test
    public void addHeaderIfValueIsNotEmptyShouldNotAddHeaderIfValueIsEmpty() {
        // given
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap();

        // when
        HttpUtil.addHeaderIfValueIsNotEmpty(headers, "header", "");

        // then
        assertThat(headers).isEmpty();
    }

    @Test
    public void addHeaderIfValueIsNotEmptyShouldNotAddHeaderIfValueIsNull() {
        // given
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap();

        // when
        HttpUtil.addHeaderIfValueIsNotEmpty(headers, "header", null);

        // then
        assertThat(headers).isEmpty();
    }

    @Test
    public void getDomainFromUrlShouldReturnDomain() {
        // given and when
        final String domain = HttpUtil.getDomainFromUrl("http://rubicon.com/ad");

        // then
        assertThat(domain).isEqualTo("rubicon.com");
    }

    @Test
    public void getDomainFromUrlShouldReturnNullIfUrlIsMalformed() {
        // given and when
        final String domain = HttpUtil.getDomainFromUrl("rubicon.com");

        // then
        assertThat(domain).isNull();
    }

    @Test
    public void cookiesAsMapShouldReturnExpectedResult() {
        // given
        given(routingContext.cookieMap()).willReturn(singletonMap("name", Cookie.cookie("name", "value")));

        // when
        final Map<String, String> cookies = HttpUtil.cookiesAsMap(routingContext);

        // then
        assertThat(cookies).hasSize(1)
                .containsOnly(entry("name", "value"));
    }

    @Test
    public void toSetCookieHeaderValueShouldReturnExpectedString() {
        // given
        final Cookie cookie = Cookie.cookie("cookie", "value")
                .setPath("/")
                .setDomain("rubicon.com");

        // when
        final String setCookieHeaderValue = HttpUtil.toSetCookieHeaderValue(cookie);

        // then
        assertThat(setCookieHeaderValue).isEqualTo("cookie=value; Path=/; Domain=rubicon.com; SameSite=None; Secure");
    }

    @Test
    public void getDateFromHeaderShouldReturnDate() {
        // given
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap().add("date-header",
                "2019-11-04T13:31:24.365+02:00[Europe/Kiev]");

        // when
        final ZonedDateTime result = HttpUtil.getDateFromHeader(headers, "date-header");

        // then
        assertThat(result).isEqualTo(ZonedDateTime.parse("2019-11-04T13:31:24.365+02:00[Europe/Kiev]"));
    }

    @Test
    public void getDateFromHeaderShouldThrowExceptionWhenHeaderWasNotFound() {
        // given
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap();

        // when
        final ZonedDateTime result = HttpUtil.getDateFromHeader(headers, "not-exist");

        // then
        assertThat(result).isNull();
    }

    @Test
    public void getDateFromHeaderShouldThrowExceptionWhenHeaderHasIncorrectFormat() {
        // given
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap().add("date-header", "invalid");

        // when and then
        assertThatThrownBy(() -> HttpUtil.getDateFromHeader(headers, "date-header"))
                .isInstanceOf(PreBidException.class)
                .hasMessage("date-header header is not compatible to ISO-8601 format: invalid");
    }
}
