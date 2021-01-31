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

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime

import org.adealsystems.platform.TimeHandling

class TimeHandlingSpec extends Specification {
    private static final long MILLIS_2020_02_12 = 1581502711275L

    @Unroll
    def "convertToLocalDate(#input) returns #expectedResult"() {
        expect:
        expectedResult == TimeHandling.convertToLocalDate(input)

        where:
        input                                                   | expectedResult
        20200131                                                | LocalDate.of(2020, 1, 31)
        "20200131"                                              | LocalDate.of(2020, 1, 31)
        "2020-01-31"                                            | LocalDate.of(2020, 1, 31)
        "2020.01.31"                                            | LocalDate.of(2020, 1, 31)
        "31.01.2020"                                            | LocalDate.of(2020, 1, 31)
        LocalDate.of(2020, 2, 12)                               | LocalDate.of(2020, 2, 12)
        Timestamp.valueOf(LocalDateTime.of(2020, 2, 12, 3, 45)) | LocalDate.of(2020, 2, 12)
        new java.sql.Date(MILLIS_2020_02_12)                    | LocalDate.of(2020, 2, 12)
        new Date(MILLIS_2020_02_12)                             | LocalDate.of(2020, 2, 12)
        ""                                                      | null
        "some arbitrary data"                                   | null
    }

    @Unroll
    def "parseWeekFromWeekOfYear(#input) returns #expectedResult"() {
        expect:
        expectedResult == TimeHandling.parseWeekFromWeekOfYear(input)
        where:
        input     | expectedResult
        "2020W17" | 17
        "22W50"   | 50
        null      | null
    }

    def "parseWeekFromWeekOfYear() throws expected exception"() {
        when:
        TimeHandling.parseWeekFromWeekOfYear("invalid input")

        then:
        IllegalArgumentException ex = thrown()
        ex.message == "weekOfYear 'invalid input' does not match either '(\\d{2})W(\\d{2})' or '(\\d{4})W(\\d{2})'!"
    }

    @Unroll
    def "parseYearFromWeekOfYear(#input) returns #expectedResult"() {
        expect:
        expectedResult == TimeHandling.parseYearFromWeekOfYear(input)
        where:
        input     | expectedResult
        "2020W17" | 2020
        "22W50"   | 2022
        null      | null
    }

    def "parseYearFromWeekOfYear() throws expected exception"() {
        when:
        TimeHandling.parseYearFromWeekOfYear("invalid input")

        then:
        IllegalArgumentException ex = thrown()
        ex.message == "weekOfYear 'invalid input' does not match either '(\\d{2})W(\\d{2})' or '(\\d{4})W(\\d{2})'!"
    }
}
