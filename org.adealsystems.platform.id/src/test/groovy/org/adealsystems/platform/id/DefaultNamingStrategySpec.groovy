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

package org.adealsystems.platform.id

import org.adealsystems.platform.time.TimeHandling
import spock.lang.Specification

import java.time.LocalDate

import static org.adealsystems.platform.id.DataFormat.CSV_SEMICOLON

class DefaultNamingStrategySpec extends Specification {

    def 'with default formatter - createLocalFilePart(#dataIdentifier, #date) returns "#expectedResult".'() {
        given:
        def dataInstance = new DataInstance(createDataResolutionStrategy(), dataIdentifier, date)
        def instance = new DefaultNamingStrategy()

        when:
        def result = instance.createLocalFilePart(dataInstance)

        then:
        result == expectedResult

        where:
        dataIdentifier                                                    | date                     | expectedResult
        new DataIdentifier("source", "use_case", CSV_SEMICOLON)           | LocalDate.of(2020, 4, 6) | "source/2020.04.06/use_case/source_use_case.csv"
        new DataIdentifier("source", "use_case", null, CSV_SEMICOLON)     | LocalDate.of(2020, 4, 6) | "source/2020.04.06/use_case/source_use_case.csv"
        new DataIdentifier("source", "use_case", CSV_SEMICOLON)           | null                     | "source/current/use_case/source_use_case.csv"
        new DataIdentifier("source", "use_case", null, CSV_SEMICOLON)     | null                     | "source/current/use_case/source_use_case.csv"
        new DataIdentifier("source", "use_case", "config", CSV_SEMICOLON) | LocalDate.of(2020, 4, 6) | "source/2020.04.06/use_case/source_use_case_config.csv"
        new DataIdentifier("source", "use_case", "config", CSV_SEMICOLON) | null                     | "source/current/use_case/source_use_case_config.csv"
    }

    def 'with explicit formatter - createLocalFilePart(#dataIdentifier, #date) returns "#expectedResult".'() {
        given:
        def dataInstance = new DataInstance(createDataResolutionStrategy(), dataIdentifier, date)
        def instance = new DefaultNamingStrategy(TimeHandling.YYYY_DASH_MM_DASH_DD_DATE_FORMATTER)

        when:
        def result = instance.createLocalFilePart(dataInstance)

        then:
        result == expectedResult

        where:
        dataIdentifier                                                    | date                     | expectedResult
        new DataIdentifier("source", "use_case", CSV_SEMICOLON)           | LocalDate.of(2020, 4, 6) | "source/2020-04-06/use_case/source_use_case.csv"
        new DataIdentifier("source", "use_case", null, CSV_SEMICOLON)     | LocalDate.of(2020, 4, 6) | "source/2020-04-06/use_case/source_use_case.csv"
        new DataIdentifier("source", "use_case", CSV_SEMICOLON)           | null                     | "source/current/use_case/source_use_case.csv"
        new DataIdentifier("source", "use_case", null, CSV_SEMICOLON)     | null                     | "source/current/use_case/source_use_case.csv"
        new DataIdentifier("source", "use_case", "config", CSV_SEMICOLON) | LocalDate.of(2020, 4, 6) | "source/2020-04-06/use_case/source_use_case_config.csv"
        new DataIdentifier("source", "use_case", "config", CSV_SEMICOLON) | null                     | "source/current/use_case/source_use_case_config.csv"
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
