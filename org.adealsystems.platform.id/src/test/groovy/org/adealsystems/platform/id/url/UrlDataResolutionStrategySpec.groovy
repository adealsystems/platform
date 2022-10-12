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

package org.adealsystems.platform.id.url

import org.adealsystems.platform.id.DataIdentifier
import org.adealsystems.platform.id.DataInstance
import org.adealsystems.platform.id.DataResolutionCapability
import org.adealsystems.platform.id.DataResolver
import org.adealsystems.platform.id.DefaultNamingStrategy
import spock.lang.Specification

import static org.adealsystems.platform.id.DataFormat.CSV_SEMICOLON

class UrlDataResolutionStrategySpec extends Specification {

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
        "s3://foo"  | "s3://foo/source/current/use_case/source_use_case.csv"
        "s3://foo/" | "s3://foo/source/current/use_case/source_use_case.csv"
    }

    def "instance has expected capabilities"() {
        given:
        UrlDataResolutionStrategy instance = createInstance("s3://foo/")

        expect:
        instance.capabilities == [DataResolutionCapability.PATH] as Set
    }

    def "instance #message capability #capability"(DataResolutionCapability capability) {
        given:
        UrlDataResolutionStrategy instance = createInstance("s3://foo/")

        expect:
        instance.supports(capability) == expectedResult

        where:
        capability << DataResolutionCapability.values()
        expectedResult << [true, false, false]
        message = expectedResult ? "supports" : "does not support"
    }

    def 'expected capability exceptions are thrown'() {
        given:
        DataResolver dataResolver = createDataResolver("s3://foo/")
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)

        when:
        dataInstance.inputStream
        then:
        UnsupportedOperationException ex = thrown()
        ex.message == "Streams are not supported by UrlDataResolutionStrategy!"

        when:
        dataInstance.outputStream
        then:
        ex = thrown()
        ex.message == "Streams are not supported by UrlDataResolutionStrategy!"

        when:
        dataInstance.delete()
        then:
        ex = thrown()
        ex.message == "Delete is not supported by UrlDataResolutionStrategy!"
    }

    private static UrlDataResolutionStrategy createInstance(String baseUrl) {
        return new UrlDataResolutionStrategy(new DefaultNamingStrategy(), baseUrl)
    }

    private static DataResolver createDataResolver(String baseUrl) {
        return new DataResolver(createInstance(baseUrl))
    }
}
