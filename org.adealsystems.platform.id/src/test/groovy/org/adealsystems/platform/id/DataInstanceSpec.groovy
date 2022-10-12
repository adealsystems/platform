/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

import java.time.LocalDate

class DataInstanceSpec extends Specification {

    def "expected exception is thrown for missing #missing"() {
        when:
        new DataInstance(null, DataIdentifier.fromString("source:use_case:CSV_COMMA"), null)

        then:
        NullPointerException ex = thrown()
        ex.message == "dataResolutionStrategy must not be null!"

        where:
        missing                  | dataResolutionStrategy         | dataIdentifier                                         | expectedMessage
        "dataResolutionStrategy" | null                           | DataIdentifier.fromString("source:use_case:CSV_COMMA") | "dataResolutionStrategy must not be null!"
        "dataIdentifier"         | createDataResolutionStrategy() | null                                                   | "dataIdentifier must not be null!"
    }

    def 'Comparable: #inputString with date #inputDate is #description #otherString with date #otherDate'() {
        given:
        DataInstance input = new DataInstance(createDataResolutionStrategy(), DataIdentifier.fromString(inputString), inputDate)
        DataInstance other = new DataInstance(createDataResolutionStrategy(), DataIdentifier.fromString(otherString), otherDate)

        when:
        def result = input <=> other

        then:
        !(isLess && isGreater) // sanity check
        isLess == (result < 0)
        isGreater == (result > 0)
        (!isLess && !isGreater) == (result == 0)

        where:
        inputString                            | inputDate                  | otherString                            | otherDate                  | isLess | isGreater
        "source:use_case:config:CSV_SEMICOLON" | null                       | "source:use_case:config:CSV_SEMICOLON" | null                       | false  | false
        "source:use_case:config:CSV_SEMICOLON" | LocalDate.of(2020, 05, 10) | "source:use_case:config:CSV_SEMICOLON" | LocalDate.of(2020, 05, 10) | false  | false
        "source:use_case:config:CSV_SEMICOLON" | null                       | "source:use_case:config:CSV_SEMICOLON" | LocalDate.of(2020, 05, 10) | true   | false
        "source:use_case:config:CSV_SEMICOLON" | LocalDate.of(2020, 05, 9)  | "source:use_case:config:CSV_SEMICOLON" | LocalDate.of(2020, 05, 10) | true   | false
        "source:use_case:config:CSV_SEMICOLON" | LocalDate.of(2020, 05, 10) | "source:use_case:config:CSV_SEMICOLON" | LocalDate.of(2020, 05, 9)  | false  | true
        "source:use_case:config:CSV_SEMICOLON" | LocalDate.of(2020, 05, 10) | "source:use_case:config:CSV_SEMICOLON" | null                       | false  | true

        description = isLess ? "less than" : isGreater ? "greater than" : "equal to"
    }

    def 'Comparable: compareTo(null) throws expected exception'() {
        given:
        DataInstance instance = new DataInstance(createDataResolutionStrategy(), DataIdentifier.fromString("source:use_case:config:CSV_SEMICOLON"), null)

        when:
        //noinspection ChangeToOperator
        instance.compareTo(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "other must not be null!"
    }

    private static DataResolutionStrategy createDataResolutionStrategy() {
        return new DataResolutionStrategy() {
            @Override
            String getPath(DataInstance dataInstance) {
                return null
            }

            @Override
            OutputStream getOutputStream(DataInstance dataInstance) throws IOException {
                return null
            }

            @Override
            InputStream getInputStream(DataInstance dataInstance) throws IOException {
                return null
            }

            @Override
            boolean delete(DataInstance dataInstance) throws IOException {
                return false
            }

            @Override
            Set<DataResolutionCapability> getCapabilities() {
                return []
            }
        }
    }
}
