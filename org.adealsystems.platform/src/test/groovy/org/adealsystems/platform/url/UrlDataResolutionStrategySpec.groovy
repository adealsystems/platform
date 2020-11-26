/*
 * Copyright 2020 ADEAL Systems GmbH
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

package org.adealsystems.platform.url

import static org.adealsystems.platform.DataFormat.CSV_SEMICOLON

import org.adealsystems.platform.DataIdentifier
import org.adealsystems.platform.DataInstance
import org.adealsystems.platform.DataResolver
import org.adealsystems.platform.DefaultNamingStrategy
import org.adealsystems.platform.url.UrlDataResolutionStrategy

import spock.lang.Specification
import spock.lang.Unroll

class UrlDataResolutionStrategySpec extends Specification {
    @Unroll
    def 'baseUrl #baseUrl produces #expectedResult'() {
        given:
        DataResolver dataResolver = createDataResolver(baseUrl)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)

        when:
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)

        then:
        dataInstance.getPath() == expectedResult

        where:
        baseUrl     | expectedResult
        "s3://foo"  | Optional.of("s3://foo/source/current/use_case/source_use_case.csv")
        "s3://foo/" | Optional.of("s3://foo/source/current/use_case/source_use_case.csv")
    }

    private static DataResolver createDataResolver(String baseUrl) {
        return new DataResolver(new UrlDataResolutionStrategy(new DefaultNamingStrategy(), baseUrl))
    }
}
