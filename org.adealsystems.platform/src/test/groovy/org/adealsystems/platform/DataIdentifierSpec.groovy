/*
 * Copyright 2020-2021 ADEAL Systems GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.adealsystems.platform

import static org.adealsystems.platform.DataFormat.CSV_COMMA
import static org.adealsystems.platform.DataFormat.CSV_SEMICOLON

import org.adealsystems.platform.DataIdentifier

import spock.lang.Specification
import spock.lang.Unroll

class DataIdentifierSpec extends Specification {
    @Unroll
    def 'new DataIdentifier(#source, #useCase, #configuration, dataFormat) works as expected'() {
        when:
        def instance = new DataIdentifier(source, useCase, configuration, dataFormat)

        then:
        instance.getSource() == source
        instance.getUseCase() == useCase
        instance.getDataFormat() == dataFormat
        instance.getConfiguration() == (configuration == null ? Optional.empty() : Optional.of(configuration))

        where:
        source   | useCase    | configuration | dataFormat
        "source" | "use_case" | "config"      | CSV_COMMA
        "source" | "use_case" | null          | CSV_COMMA
    }

    @Unroll
    def 'new DataIdentifier(#source, #useCase, #configuration, dataFormat) throws expected exception of #expectedExceptionType'() {
        when:
        new DataIdentifier(source, useCase, configuration, dataFormat)

        then:
        Exception ex = thrown()
        ex.class == expectedExceptionType
        ex.message == expectedMessage


        where:
        source    | useCase    | configuration | dataFormat | expectedExceptionType    | expectedMessage
        null      | "use_case" | "config"      | CSV_COMMA  | NullPointerException     | "source must not be null!"
        "source"  | null       | "config"      | CSV_COMMA  | NullPointerException     | "useCase must not be null!"
        "source"  | "use_case" | "config"      | null       | NullPointerException     | "dataFormat must not be null!"
        "_broken" | "use_case" | "config"      | CSV_COMMA  | IllegalArgumentException | "source value '_broken' doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "'!"
        "source"  | "_broken"  | "config"      | CSV_COMMA  | IllegalArgumentException | "useCase value '_broken' doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "'!"
        "source"  | "use_case" | "_broken"     | CSV_COMMA  | IllegalArgumentException | "configuration value '_broken' doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "'!"
    }

    @Unroll
    def '#original withConfiguration(#configuration) returns #expectedResult.'() {
        when:
        def result = original.withConfiguration(configuration)

        then:
        result == expectedResult

        where:
        original                                                  | configuration | expectedResult
        new DataIdentifier("foo", "bar", CSV_SEMICOLON)           | "config"      | new DataIdentifier("foo", "bar", "config", CSV_SEMICOLON)
        new DataIdentifier("foo", "bar", "config", CSV_SEMICOLON) | null          | new DataIdentifier("foo", "bar", CSV_SEMICOLON)
    }

    @Unroll
    def '#original withDataFormat(#dataFormat) returns #expectedResult.'() {
        when:
        def result = original.withDataFormat(dataFormat)

        then:
        result == expectedResult

        where:
        original                                                  | dataFormat | expectedResult
        new DataIdentifier("foo", "bar", CSV_SEMICOLON)           | CSV_COMMA  | new DataIdentifier("foo", "bar", CSV_COMMA)
        new DataIdentifier("foo", "bar", "config", CSV_SEMICOLON) | CSV_COMMA  | new DataIdentifier("foo", "bar", "config", CSV_COMMA)
    }

    @Unroll
    def 'checkElement(#name, #value, #optional) works'() {
        when:
        String result = DataIdentifier.checkElement(name, value, optional)

        then:
        result == value

        where:
        name  | value       | optional
        "foo" | "a_b"       | false
        "foo" | "abc_def_0" | false
        "foo" | null        | true
    }

    @Unroll
    def 'checkElement(#name, #value, #optional) throws expected exception of #expectedExceptionType'() {
        when:
        DataIdentifier.checkElement(name, value, optional)

        then:
        Exception ex = thrown()
        ex.class == expectedExceptionType
        ex.message == expectedMessage

        where:
        name  | value    | optional | expectedExceptionType    | expectedMessage
        null  | null     | false    | NullPointerException     | "name must not be null!"
        "foo" | null     | false    | NullPointerException     | "foo must not be null!"
        "foo" | "_abc"   | false    | IllegalArgumentException | "foo value '_abc' doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "'!"
        "foo" | "a__abc" | false    | IllegalArgumentException | "foo value 'a__abc' doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "'!"
        "foo" | "a_abc_" | false    | IllegalArgumentException | "foo value 'a_abc_' doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "'!"
        "foo" | "0_abc"  | false    | IllegalArgumentException | "foo value '0_abc' doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "'!"
    }

    @Unroll
    def 'DataIdentifier.fromString(#input) produces expected result #expectedResult'() {
        when:
        DataIdentifier value = DataIdentifier.fromString(input)

        then:
        value == expectedResult
        value.toString() == input
        value.toString() == expectedResult.toString()

        where:
        input                                  | expectedResult
        "source:use_case:config:CSV_SEMICOLON" | new DataIdentifier("source", "use_case", "config", CSV_SEMICOLON)
        "source:use_case:CSV_SEMICOLON"        | new DataIdentifier("source", "use_case", CSV_SEMICOLON)
    }

    @Unroll
    def 'DataIdentifier.fromString(#input) produces expected exception of #expectedException'() {
        when:
        DataIdentifier.fromString(input)

        then:
        Exception ex = thrown()
        ex.class == expectedException
        ex.message == expectedMessage

        where:
        input                      | expectedException        | expectedMessage
        null                       | NullPointerException     | "input must not be null!"
        "source"                   | IllegalArgumentException | "Expected three or four tokens separated by ':' but got 1!"
        "source:"                  | IllegalArgumentException | "Expected three or four tokens separated by ':' but got 1!"
        "source:use_case"          | IllegalArgumentException | "Expected three or four tokens separated by ':' but got 2!"
        "source:use_case:x:y:z"    | IllegalArgumentException | "Expected three or four tokens separated by ':' but got 5!"
        "source:use_case:config:X" | IllegalArgumentException | "No enum constant org.adealsystems.platform.DataFormat.X"
    }

    @Unroll
    def 'Comparable: #inputString is #description #otherString'() {
        given:
        DataIdentifier input = DataIdentifier.fromString(inputString)
        DataIdentifier other = DataIdentifier.fromString(otherString)

        when:
        def result = input <=> other

        then:
        !(isLess && isGreater) // sanity check
        isLess == (result < 0)
        isGreater == (result > 0)
        (!isLess && !isGreater) == (result == 0)

        where:
        inputString                            | otherString                            | isLess | isGreater
        "source:use_case:config:CSV_SEMICOLON" | "source:use_case:config:CSV_SEMICOLON" | false  | false
        "source:use_case:CSV_SEMICOLON"        | "source:use_case:config:CSV_SEMICOLON" | true   | false
        "source:use_case:config:CSV_SEMICOLON" | "source:use_case:CSV_SEMICOLON"        | false  | true
        "source:use_case:CSV_SEMICOLON"        | "source:use_case:CSV_SEMICOLON"        | false  | false
        "a:use_case:config:CSV_SEMICOLON"      | "b:use_case:config:CSV_SEMICOLON"      | true   | false
        "source:a:config:CSV_SEMICOLON"        | "source:b:config:CSV_SEMICOLON"        | true   | false
        "source:use_case:a:CSV_SEMICOLON"      | "source:use_case:b:CSV_SEMICOLON"      | true   | false
        "source:use_case:config:CSV_COMMA"     | "source:use_case:config:CSV_SEMICOLON" | true   | false
        "source:use_case:config:CSV_SEMICOLON" | "source:use_case:config:CSV_COMMA"     | false  | true
        "source:use_case:CSV_COMMA"            | "source:use_case:CSV_SEMICOLON"        | true   | false
        "source:use_case:CSV_SEMICOLON"        | "source:use_case:CSV_COMMA"            | false  | true

        description = isLess ? "less than" : isGreater ? "greater than" : "equal to"
    }

    def 'Comparable: compareTo(null) throws expected exception'() {
        given:
        def instance = DataIdentifier.fromString("source:use_case:config:CSV_SEMICOLON")

        when:
        //noinspection ChangeToOperator
        instance.compareTo(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "other must not be null!"
    }
}
