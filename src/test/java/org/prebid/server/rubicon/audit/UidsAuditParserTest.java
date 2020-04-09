package org.prebid.server.rubicon.audit;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.prebid.server.rubicon.audit.proto.UidAudit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UidsAuditParserTest {

    @Test
    public void parseCookieValueShouldReturnUidAudit() throws InvalidAuditFormatException {
        // given and when
        final UidAudit uidAudit = UidsAuditParser.parseCookieValue(
                "1|uid^19243234^19234353^ua^1527684614^referrer|initiatorType^initiatorId|1^consent");

        // then
        assertThat(uidAudit).isEqualTo(UidAudit.builder().version("1").uid("uid").hostIp("19243234").userIp("19234353")
                .country("ua").renewedSeconds(1527684614L).referringDomain("referrer").initiatorType("initiatorType")
                .initiatorId("initiatorId").consentUsed("1").consent("consent").build());
    }

    @Test
    public void parseCookieValueShouldThrowInvalidAuditFormatExceptionIfVersionWasNotCorrect() {
        // given, when and then
        assertThatThrownBy(() -> UidsAuditParser.parseCookieValue(
                "2|uid^19243234^19234353^ua^1527684614^referrer|initiatorType^initiatorId|1^consent"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Uids audit cookie has invalid version = 2");
    }

    @Test
    public void parseCookieValueShouldReturnEmptyGroupInUidAuditIfGroupIsEmptyInString()
            throws InvalidAuditFormatException {
        // given and when
        final UidAudit uidAudit = UidsAuditParser.parseCookieValue("1|^^^^^|^|0^");

        // then
        assertThat(uidAudit).isEqualTo(UidAudit.builder().version("1").consentUsed("0").build());
    }

    @Test
    public void parseCookieValueShouldHandleCaseWithLastMissedValueInGroup() throws InvalidAuditFormatException {
        // given and when
        final UidAudit uidAudit = UidsAuditParser.parseCookieValue(
                "1|uid^19243234^19234353^ua^1527684614^|initiatorType^|1^");

        // then
        assertThat(uidAudit).isEqualTo(UidAudit.builder().version("1").uid("uid").hostIp("19243234").userIp("19234353")
                .country("ua").renewedSeconds(1527684614L).initiatorType("initiatorType").consentUsed("1").build());
    }

    @Test
    public void parseCookieShouldHandleCaseWhenFirstElementInGroupWasMissed() throws InvalidAuditFormatException {
        // given and when
        final UidAudit uidAudit = UidsAuditParser.parseCookieValue(
                "1|uid^19243234^19234353^ua^1527684614^referrer|^initiatorId|1^consent");

        // then
        assertThat(uidAudit).isEqualTo(UidAudit.builder().version("1").uid("uid").hostIp("19243234").userIp("19234353")
                .country("ua").renewedSeconds(1527684614L).referringDomain("referrer").initiatorId("initiatorId")
                .consentUsed("1").consent("consent").build());
    }

    @Test
    public void parseCookieValueShouldThrowInvalidAuditFormatExceptionWhenConsentIsNotDefined() {
        // given, when and then
        assertThatThrownBy(() -> UidsAuditParser.parseCookieValue(
                "1|^19243234^19234353^ua^1527684614^referrer|^initiatorId|^consent"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Consent flag should be present and equals to 0 or 1, but actually was \"\"");
    }

    @Test
    public void parseCookieValueShouldThrowInvalidAuditFormatExceptionWhenConsentIsNotValidValue() {
        // given, when and then
        assertThatThrownBy(() -> UidsAuditParser.parseCookieValue(
                "1|^19243234^19234353^ua^1527684614^referrer|^initiatorId|5^consent"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Consent flag should be present and equals to 0 or 1, but actually was \"5\"");
    }

    @Test
    public void parseCookieValuesShouldThrowAuditFormatExceptionWhenGroupsSizeForVersionOneStructureIncorrect() {
        assertThatThrownBy(() -> UidsAuditParser.parseCookieValue("1|^19243234^19234353^ua^1527684614"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Expected groups number for cookie structure version #1 is 4, but actually was 2");
    }

    @Test
    public void parseCookieValuesShouldThrowAuditFormatExceptionWhenSecondGroupIncorrectFieldsNumber() {
        assertThatThrownBy(() -> UidsAuditParser.parseCookieValue("1|^19243234^19234353^ua^1527684614|^|5^consent"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Expected group fields number for group #2 is 6, but actually was 5");
    }

    @Test
    public void parseCookieValuesShouldThrowAuditFormatExceptionWhenThirdGroupIncorrectFieldsNumber() {
        assertThatThrownBy(() -> UidsAuditParser.parseCookieValue("1|^^^^^||5^consent"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Expected group fields number for group #3 is 2, but actually was 1");
    }

    @Test
    public void parseCookieValuesShouldThrowAuditFormatExceptionWhenFourthGroupIncorrectFieldsNumber() {
        assertThatThrownBy(() -> UidsAuditParser.parseCookieValue("1|^^^^^|^|^^"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Expected group fields number for group #4 is 2, but actually was 3");
    }

    @Test
    public void getMaskedDecimalIpShouldReturnCorrectIpv4MaskedValue() throws InvalidAuditFormatException {
        // given and when
        final String maskedIp = UidsAuditParser.getMaskedDecimalIp("192.168.111.123");

        // then
        // 3232263936 is decimal 192.168.111.000
        assertThat(maskedIp).isEqualTo("3232263936");
    }

    @Test
    public void getMaskedDecimalIpShouldReturnCorrectIpv6MaskedValue() throws InvalidAuditFormatException {
        // given and when
        final String maskedIp = UidsAuditParser.getMaskedDecimalIp("2001:0db8:85a3:0000:0000:8a2e:0370:7334");

        // then
        // 42540766452641154071740215577757614080 is decimal 2001:0db8:85a3:0000:0000:8a2e:0370:0000
        assertThat(maskedIp).isEqualTo("42540766452641154071740215577757614080");
    }

    @Test
    public void getMaskedDecimalIpShouldThrowInvalidAuditFormatExceptionIfIpInvalid() {
        // given, when and then
        assertThatThrownBy(() -> UidsAuditParser.getMaskedDecimalIp("invalid"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Incorrect ip address format - \"invalid\".");
    }

    @Test
    public void getMaskedIpShouldThrowInvalidAuditFormatExceptionIfIpIsEmptyString() {
        // given, when and then
        assertThatThrownBy(() -> UidsAuditParser.getMaskedDecimalIp(""))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Incorrect ip address format - \"\".");
    }

    @Test
    public void getMaskedIpShouldReturnEmptyStringIfIpIsNull() throws InvalidAuditFormatException {
        // given and when
        final String maskedIp = UidsAuditParser.getMaskedDecimalIp(null);

        // then
        assertThat(maskedIp).isEqualTo("");
    }

    @Test
    public void ipToDecimalShouldThrowInvalidAuditFormatExceptionIfIpIsInvalid() {
        // given, when and then
        assertThatThrownBy(() -> UidsAuditParser.ipToDecimal("invalid"))
                .isExactlyInstanceOf(InvalidAuditFormatException.class)
                .hasMessage("Incorrect ip address format - 'invalid'.");
    }

    @Test
    public void ipToDecimalShouldReturnDecimalIpRepresentation() throws InvalidAuditFormatException {
        // given and when
        final String decimalIp = UidsAuditParser.ipToDecimal("192.168.0.1");

        // then
        assertThat(decimalIp).isEqualTo("3232235521");
    }

    @Test
    public void uidAuditToRowShouldReturnStringWithAllValues() {
        // given
        final UidAudit uidAudit = UidAudit.builder().version("1").uid("uid").hostIp("3232235521").userIp("3232235522")
                .country("ua").renewedSeconds(123456L).referringDomain("referrer").initiatorType("initiatorType")
                .initiatorId("initiatorId").consentUsed("1").consent("sfasdfasdfd").build();

        // when
        final String auditRow = UidsAuditParser.uidAuditToRow(uidAudit);

        // then
        assertThat(auditRow)
                .isEqualTo("1|uid^3232235521^3232235522^ua^123456^referrer|initiatorType^initiatorId|1^sfasdfasdfd");
    }

    @Test
    public void uidAuditToRowShouldReturnCorrectValueIfFirstValueOfGroupWasMissed() {
        // given
        final UidAudit uidAudit = UidAudit.builder().version("1").hostIp("3232235521").userIp("3232235522")
                .country("ua").renewedSeconds(123456L).referringDomain("referrer")
                .initiatorId("initiatorId").consentUsed("1").consent("sfasdfasdfd").build();

        // when
        final String auditRow = UidsAuditParser.uidAuditToRow(uidAudit);

        // then
        assertThat(auditRow)
                .isEqualTo("1|^3232235521^3232235522^ua^123456^referrer|^initiatorId|1^sfasdfasdfd");
    }

    @Test
    public void uidAuditToRowShouldReturnCorrectValueIfLastValueOfGroupWasMissed() {
        // given
        final UidAudit uidAudit = UidAudit.builder().version("1").uid("uid").hostIp("3232235521").userIp("3232235522")
                .country("ua").renewedSeconds(123456L).initiatorType("initiatorType")
                .consentUsed("1").build();

        // when
        final String auditRow = UidsAuditParser.uidAuditToRow(uidAudit);

        // then
        assertThat(auditRow)
                .isEqualTo("1|uid^3232235521^3232235522^ua^123456^|initiatorType^|1^");
    }

    @Test
    public void uidAuditToRowShouldReturnTrimmedConsentStringIfItIsLargerThenOneHundred() {
        // given
        final UidAudit uidAudit = UidAudit.builder().version("1").uid("uid").hostIp("3232235521").userIp("3232235522")
                .country("ua").renewedSeconds(123456L).initiatorType("initiatorType")
                .consentUsed("1").consent(StringUtils.repeat('a', 101)).build();

        // when
        final String auditRow = UidsAuditParser.uidAuditToRow(uidAudit);

        // then
        assertThat(auditRow)
                .isEqualTo(String.format(
                        "1|uid^3232235521^3232235522^ua^123456^|initiatorType^|1^%s", StringUtils.repeat('a', 100)));
    }

    @Test
    public void uidAuditToRowShouldHandleCaseWhenValueMissedInTheMiddleOfTheGroup() {
        // given
        final UidAudit uidAudit = UidAudit.builder().version("1").uid("uid").hostIp("3232235521").userIp("3232235522")
                .referringDomain("referrer").initiatorType("initiatorType")
                .initiatorId("initiatorId").consentUsed("1").consent("asd").build();

        // when
        final String auditRow = UidsAuditParser.uidAuditToRow(uidAudit);

        // then
        assertThat(auditRow)
                .isEqualTo("1|uid^3232235521^3232235522^^^referrer|initiatorType^initiatorId|1^asd");
    }

    @Test
    public void uidAuditToRowShouldSkipConsentStringIfConsentUsedIsZero() {
        // given
        final UidAudit uidAudit = UidAudit.builder().version("1").uid("uid").hostIp("3232235521").userIp("3232235522")
                .country("ua").renewedSeconds(123456L).referringDomain("referrer").initiatorType("initiatorType")
                .initiatorId("initiatorId").consentUsed("0").consent("sfasdfasdfd").build();

        // when
        final String auditRow = UidsAuditParser.uidAuditToRow(uidAudit);

        // then
        assertThat(auditRow)
                .isEqualTo("1|uid^3232235521^3232235522^ua^123456^referrer|initiatorType^initiatorId|0^");
    }
}
