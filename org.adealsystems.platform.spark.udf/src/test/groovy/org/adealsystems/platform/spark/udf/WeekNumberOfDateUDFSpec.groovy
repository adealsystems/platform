/*
 * Copyright 2020-2025 ADEAL Systems GmbH
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

class WeekNumberOfDateUDFSpec extends Specification {
    private static final long MILLIS_2020_02_12 = 1581502711275L


    def "weekNumberOfDateUDF.call(#input) returns #expectedResult"() {
        given:
        def instance = new WeekNumberOfDateUDF()
        expect:
        expectedResult == instance.call(input)

        where:
        input                                                   | expectedResult
        20200131                                                | 5
        "20200131"                                              | 5
        "2020-01-31"                                            | 5
        "2020.01.31"                                            | 5
        "31.01.2022"                                            | 5
        LocalDate.of(2020, 2, 12)                               | 7
        Timestamp.valueOf(LocalDateTime.of(2020, 2, 12, 3, 45)) | 7
        new java.sql.Date(MILLIS_2020_02_12)                    | 7
        new Date(MILLIS_2020_02_12)                             | 7
        ""                                                      | null
        "some arbitrary data"                                   | null
    }
}
