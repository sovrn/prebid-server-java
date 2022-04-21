package org.prebid.server.functional.tests

import org.prebid.server.functional.model.Currency
import org.prebid.server.functional.model.mock.services.currencyconversion.CurrencyConversionRatesResponse
import org.prebid.server.functional.repository.HibernateRepositoryService
import org.prebid.server.functional.repository.dao.AccountDao
import org.prebid.server.functional.repository.dao.ConfigDao
import org.prebid.server.functional.repository.dao.StoredImpDao
import org.prebid.server.functional.repository.dao.StoredRequestDao
import org.prebid.server.functional.repository.dao.StoredResponseDao
import org.prebid.server.functional.service.PrebidServerService
import org.prebid.server.functional.testcontainers.Dependencies
import org.prebid.server.functional.testcontainers.PBSTest
import org.prebid.server.functional.testcontainers.PbsServiceFactory
import org.prebid.server.functional.testcontainers.scaffolding.Bidder
import org.prebid.server.functional.testcontainers.scaffolding.CurrencyConversion
import org.prebid.server.functional.testcontainers.scaffolding.FloorsProvider
import org.prebid.server.functional.testcontainers.scaffolding.PrebidCache
import org.prebid.server.functional.util.ObjectMapperWrapper
import org.prebid.server.functional.util.PBSUtils
import spock.lang.Specification

import java.math.RoundingMode

import static java.math.RoundingMode.DOWN
import static org.prebid.server.functional.model.Currency.EUR
import static org.prebid.server.functional.model.Currency.JPY
import static org.prebid.server.functional.model.Currency.USD

@PBSTest
abstract class BaseSpec extends Specification {

    protected static final Map<Currency, Map<Currency, BigDecimal>> DEFAULT_CURRENCY_RATES = [(USD): [(EUR): 0.927,
                                                                                                      (JPY): 125.393],
                                                                                              (EUR): [(USD): 1.091,
                                                                                                      (JPY): 136.885]]

    protected static final ObjectMapperWrapper mapper = Dependencies.objectMapperWrapper
    protected static final PbsServiceFactory pbsServiceFactory = new PbsServiceFactory(Dependencies.networkServiceContainer, mapper)
    protected static final Bidder bidder = new Bidder(Dependencies.networkServiceContainer, mapper)
    protected static final PrebidCache prebidCache = new PrebidCache(Dependencies.networkServiceContainer, mapper)
    protected static final FloorsProvider floorsProvider = new FloorsProvider(Dependencies.networkServiceContainer, mapper)
    protected static final PrebidServerService defaultPbsService = pbsServiceFactory.getService([:])

    protected static final CurrencyConversion currencyConversion = new CurrencyConversion(Dependencies.networkServiceContainer, mapper).tap {
        setCurrencyConversionRatesResponse(CurrencyConversionRatesResponse.getDefaultCurrencyConversionRatesResponse(DEFAULT_CURRENCY_RATES))
    }

    protected static final HibernateRepositoryService repository = new HibernateRepositoryService(Dependencies.mysqlContainer)
    protected static final AccountDao accountDao = repository.accountDao
    protected static final ConfigDao configDao = repository.configDao
    protected static final StoredImpDao storedImp = repository.storedImpDao
    protected static final StoredRequestDao storedRequestDao = repository.storedRequestDao
    protected static final StoredResponseDao storedResponseDao = repository.storedResponseDao

    protected static final String fetchUrl = Dependencies.networkServiceContainer.rootUri +
            FloorsProvider.FLOORS_ENDPOINT
    protected static final int MAX_TIMEOUT = 6000
    private static final int MIN_TIMEOUT = 5000
    private static final int DEFAULT_TARGETING_PRECISION = 1
    public static final Currency DEFAULT_CURRENCY = USD
    protected static final int PRICE_PRECISION = 3

    def setupSpec() {
        prebidCache.setResponse()
        bidder.setResponse()
    }

    def cleanupSpec() {
        bidder.reset()
        prebidCache.reset()
        repository.removeAllDatabaseData()
        pbsServiceFactory.stopContainers()
    }

    protected static int getRandomTimeout() {
        PBSUtils.getRandomNumber(MIN_TIMEOUT, MAX_TIMEOUT)
    }

    protected static Number getCurrentMetricValue(PrebidServerService pbsService = defaultPbsService, String name) {
        def response = pbsService.sendCollectedMetricsRequest()
        response[name] ?: 0
    }

    protected static void flushMetrics(PrebidServerService pbsService = defaultPbsService) {
        // flushing PBS metrics by receiving collected metrics so that each new test works with a fresh state
        pbsService.sendCollectedMetricsRequest()
    }

    protected static List<String> getLogsByText(List<String> logs, String text) {
        logs.findAll { it.contains(text) }
    }

    protected static String getRoundedTargetingValueWithDefaultPrecision(BigDecimal value) {
        "${value.setScale(DEFAULT_TARGETING_PRECISION, DOWN)}0"
    }

    protected BigDecimal getCurrencyRate(Currency currencyFrom, Currency currencyTo) {
        def response = defaultPbsService.sendCurrencyRatesRequest()
        response.rates[currencyFrom.value][currencyTo.value]
    }

    protected static BigDecimal convertCurrency(BigDecimal price, Currency fromCurrency, Currency toCurrency) {
        return (price * getConversionRate(fromCurrency, toCurrency)).setScale(PRICE_PRECISION, RoundingMode.HALF_EVEN)
    }

    protected static BigDecimal getConversionRate(Currency fromCurrency, Currency toCurrency) {
        def conversionRate
        if (fromCurrency == toCurrency) {
            conversionRate = 1
        } else if (fromCurrency in DEFAULT_CURRENCY_RATES) {
            conversionRate = DEFAULT_CURRENCY_RATES[fromCurrency][toCurrency]
        } else {
            conversionRate = 1 / DEFAULT_CURRENCY_RATES[toCurrency][fromCurrency]
        }
        conversionRate
    }
}
