package cecs429.porter2;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import cecs429.text.Normalize;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for Normalize.java
 */
class NormalizeTest {

    private final Normalize test = new Normalize(); //Test Normalize object

    /*
        Block for test cases
     */
    @DisplayName("Token normalization ParameterizedTest")
    @ParameterizedTest
    //@ValueSource(strings = {"Hello!", "!WORLD!", "test"})
    @CsvSource({"Hello!, hello", "!WORLD!, world", "test, test"})
    void testTokens(String inToken, String outToken) {
        List<String> testCase = Arrays.asList(outToken);
        assertIterableEquals(testCase, test.processToken(inToken));
    }

    @DisplayName("Test for \"???SUPERcalifragilisticexpialidociOUS\"")
    @Test
    void processToken1() {
        List<String> testCase = Arrays.asList("supercalifragilisticexpialidoci");
        assertIterableEquals(testCase, test.processToken("???SUPERcalifragilisticexpialidociOUS"));
    }

    @DisplayName("Test for \"friendly??\"")
    @Test
    void processToken2() {
        List<String> testCase = Arrays.asList("friend");
        assertIterableEquals(testCase, test.processToken("friendly??"));
    }

    @DisplayName("Test for \"chicken-nugget\"")
    @Test
    void processToken3() {
        List<String> testCase = Arrays.asList("chicken", "nugget", "chickennugget");
        assertIterableEquals(testCase, test.processToken("chicken-nugget"));
    }

    @DisplayName("Test for \"???hello\"")
    @Test
    void processToken4() {
        List<String> testCase = Arrays.asList("hello");
        assertIterableEquals(testCase, test.processToken("???hello"));
    }

    @DisplayName("Test for \"mummY\"")
    @Test
    void processToken5() {
        List<String> testCase = Arrays.asList("mummi");
        assertIterableEquals(testCase, test.processToken("mummY"));
    }

    @DisplayName("Test for \"mumMIfied\"")
    @Test
    void processToken6() {
        List<String> testCase = Arrays.asList("mummifi");
        assertIterableEquals(testCase, test.processToken("mumMIfied"));
    }

    @DisplayName("Test for \"MUMMification\"")
    @Test
    void processToken7() {
        List<String> testCase = Arrays.asList("mummif");
        assertIterableEquals(testCase, test.processToken("MUMMification"));
    }

    @DisplayName("Test for \"\"no\"\"")
    @Test
    void processToken8() {
        List<String> testCase = Arrays.asList("no");
        assertIterableEquals(testCase, test.processToken("\"no\""));
    }

    @DisplayName("Test for \"\'no\'\"")
    @Test
    void processToken9() {
        List<String> testCase = Arrays.asList("no");
        assertIterableEquals(testCase, test.processToken("\'no\'"));
    }

    @DisplayName("Test for \"!,no;!\"")
    @Test
    void processToken10() {
        List<String> testCase = Arrays.asList("no");
        assertIterableEquals(testCase, test.processToken("!,no;!"));
    }

    @DisplayName("Test for \"NO.NO\"")
    @Test
    void processToken11() {
        List<String> testCase = Arrays.asList("no.no");
        assertIterableEquals(testCase, test.processToken("NO.NO"));
    }

    @DisplayName("Test for \"ago------never\"")
    @Test
    void processToken12() {
        List<String> testCase = Arrays.asList("ago", "never", "agonev");
        assertIterableEquals(testCase, test.processToken("ago------never"));
    }

    @DisplayName("Test for \"cat's\"")
    @Test
    void processToken13() {
        List<String> testCase = Arrays.asList("cat");
        assertIterableEquals(testCase, test.processToken("cat's"));
    }

    @DisplayName("Test for \"192.168.12.01\"")
    @Test
    void processToken14() {
        List<String> testCase = Arrays.asList("192.168.12.01");
        assertIterableEquals(testCase, test.processToken("192.168.12.01"));
    }

    @DisplayName("Test for \"https://snowballstem.org/demo.html???????????????????????\"")
    @Test
    void processToken15() {
        List<String> testCase = Arrays.asList("https://snowballstem.org/demo.html");
        assertIterableEquals(testCase, test.processToken("https://snowballstem.org/demo.html???????????????????????"));
    }

    @DisplayName("Test for \"! 'google.com??\"")
    @Test
    void processToken16() {
        List<String> testCase = Arrays.asList("google.com");
        assertIterableEquals(testCase, test.processToken("! 'google.com??"));
    }

    @DisplayName("Test for \"google!;'.?\"com\"")
    @Test
    void processToken17() {
        List<String> testCase = Arrays.asList("google!;.?com");
        assertIterableEquals(testCase, test.processToken("google!;'.?\"com"));
    }

    @DisplayName("Test for \"google.--.--com\"")
    @Test
    void processToken18() {
        List<String> testCase = Arrays.asList("googl", "com", "googlecom");
        assertIterableEquals(testCase, test.processToken("google.--.--com"));
    }
}