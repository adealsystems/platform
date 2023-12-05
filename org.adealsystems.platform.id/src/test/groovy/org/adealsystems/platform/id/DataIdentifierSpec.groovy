/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

package org.adealsystems.platform.id

import spock.lang.Specification

import static org.adealsystems.platform.id.DataFormat.CSV_COMMA
import static org.adealsystems.platform.id.DataFormat.CSV_SEMICOLON

class DataIdentifierSpec extends Specification {

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

    def 'new DataIdentifier(#source, #useCase, #configuration, dataFormat) throws expected exception of #expectedExceptionType'() {
        when:
        new DataIdentifier(source, useCase, configuration, dataFormat)

        then:
        DataIdentifierCreationException ex = thrown()
        ex.class == expectedExceptionType
        ex.message == expectedMessage
        ex.getValue() == errorValue

        where:
        source    | useCase    | configuration | dataFormat | expectedExceptionType           | expectedMessage                                                                                                   | errorValue
        null      | "use_case" | "config"      | CSV_COMMA  | DataIdentifierCreationException | "source must not be null!"                                                                                        | null
        "source"  | null       | "config"      | CSV_COMMA  | DataIdentifierCreationException | "useCase must not be null!"                                                                                       | null
        "source"  | "use_case" | "config"      | null       | DataIdentifierCreationException | "dataFormat must not be null!"                                                                                    | null
        "_broken" | "use_case" | "config"      | CSV_COMMA  | DataIdentifierCreationException | "source value doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "': '_broken'!"                      | "_broken"
        "source"  | "_broken"  | "config"      | CSV_COMMA  | DataIdentifierCreationException | "useCase value doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "': '_broken'!"                     | "_broken"
        "source"  | "use_case" | "_broken"     | CSV_COMMA  | DataIdentifierCreationException | "configuration value doesn't match the pattern '" + DataIdentifier.CONFIGURATION_PATTERN_STRING + "': '_broken'!" | "_broken"
    }

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

    def 'checkElement(#name, #value, #optional) throws expected exception of #expectedExceptionType'() {
        when:
        DataIdentifier.checkElement(name, value, optional)

        then:
        DataIdentifierCreationException ex = thrown()
        ex.class == expectedExceptionType
        ex.message == expectedMessage
        ex.value == errorValue

        where:
        name  | value    | optional | expectedExceptionType           | expectedMessage                                                                          | errorValue
        null  | null     | false    | DataIdentifierCreationException | "name must not be null!"                                                                 | null
        "foo" | null     | false    | DataIdentifierCreationException | "foo must not be null!"                                                                  | null
        "foo" | "_abc"   | false    | DataIdentifierCreationException | "foo value doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "': '_abc'!"   | "_abc"
        "foo" | "a__abc" | false    | DataIdentifierCreationException | "foo value doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "': 'a__abc'!" | "a__abc"
        "foo" | "a_abc_" | false    | DataIdentifierCreationException | "foo value doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "': 'a_abc_'!" | "a_abc_"
        "foo" | "0_abc"  | false    | DataIdentifierCreationException | "foo value doesn't match the pattern '" + DataIdentifier.PATTERN_STRING + "': '0_abc'!"  | "0_abc"
    }

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

    def 'DataIdentifier.fromString(#input) produces expected exception of #expectedException'() {
        when:
        DataIdentifier.fromString(input)

        then:
        DataIdentifierCreationException ex = thrown()
        ex.class == expectedException
        ex.message == expectedMessage
        ex.value == errorValue

        where:
        input                      | expectedException               | expectedMessage                                             | errorValue
        null                       | DataIdentifierCreationException | "input must not be null!"                                   | null
        "source"                   | DataIdentifierCreationException | "Expected three or four tokens separated by ':' but got 1!" | "source"
        "source:"                  | DataIdentifierCreationException | "Expected three or four tokens separated by ':' but got 1!" | "source:"
        "source:use_case"          | DataIdentifierCreationException | "Expected three or four tokens separated by ':' but got 2!" | "source:use_case"
        "source:use_case:x:y:z"    | DataIdentifierCreationException | "Expected three or four tokens separated by ':' but got 5!" | "source:use_case:x:y:z"
        "source:use_case:config:X" | DataIdentifierCreationException | "Error creating DataFormat!"                                | "No enum constant org.adealsystems.platform.id.DataFormat.X"
    }

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
