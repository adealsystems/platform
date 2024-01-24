/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

package org.adealsystems.platform.spark.udf

import spock.lang.Specification

import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime

class WeekOfYearOfDateUDFSpec extends Specification {
    private static final long MILLIS_2020_02_12 = 1581502711275L

    def "weekOfYearOfDateUDF(longFormat=#longFormat, locale=#locale).call(#input) returns #expectedResult"() {
        given:
        def instance = new WeekOfYearOfDateUDF(longFormat, locale)
        expect:
        expectedResult == instance.call(input)

        where:
        input                                                   | longFormat | locale         | expectedResult
        20200131                                                | true       | Locale.GERMANY | "2020W05"
        "20200131"                                              | true       | Locale.GERMANY | "2020W05"
        "2020-01-31"                                            | true       | Locale.GERMANY | "2020W05"
        "2020.01.31"                                            | true       | Locale.GERMANY | "2020W05"
        "31.01.2022"                                            | true       | Locale.GERMANY | "2022W05"
        "31.01.2022"                                            | true       | Locale.US      | "2022W06"
        LocalDate.of(2020, 2, 12)                               | true       | Locale.GERMANY | "2020W07"
        Timestamp.valueOf(LocalDateTime.of(2020, 2, 12, 3, 45)) | true       | Locale.GERMANY | "2020W07"
        new java.sql.Date(MILLIS_2020_02_12)                    | true       | Locale.GERMANY | "2020W07"
        new Date(MILLIS_2020_02_12)                             | true       | Locale.GERMANY | "2020W07"
        20200131                                                | false      | Locale.GERMANY | "20W05"
        "20200131"                                              | false      | Locale.GERMANY | "20W05"
        "2020-01-31"                                            | false      | Locale.GERMANY | "20W05"
        "2020.01.31"                                            | false      | Locale.GERMANY | "20W05"
        "31.01.2022"                                            | false      | Locale.GERMANY | "22W05"
        "31.01.2022"                                            | false      | Locale.US      | "22W06"
        LocalDate.of(2020, 2, 12)                               | false      | Locale.GERMANY | "20W07"
        Timestamp.valueOf(LocalDateTime.of(2020, 2, 12, 3, 45)) | false      | Locale.GERMANY | "20W07"
        new java.sql.Date(MILLIS_2020_02_12)                    | false      | Locale.GERMANY | "20W07"
        new Date(MILLIS_2020_02_12)                             | false      | Locale.GERMANY | "20W07"
        ""                                                      | true       | Locale.GERMANY | null
        "some arbitrary data"                                   | false      | Locale.GERMANY | null
    }
}
