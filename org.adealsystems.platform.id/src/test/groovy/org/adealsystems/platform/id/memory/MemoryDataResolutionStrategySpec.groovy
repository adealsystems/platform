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

package org.adealsystems.platform.id.memory

import org.adealsystems.platform.id.DataIdentifier
import org.adealsystems.platform.id.DataInstance
import org.adealsystems.platform.id.DataResolutionCapability
import org.adealsystems.platform.id.DataResolver
import org.adealsystems.platform.id.DefaultNamingStrategy
import spock.lang.Specification

import static org.adealsystems.platform.id.DataFormat.CSV_SEMICOLON

class MemoryDataResolutionStrategySpec extends Specification {
    def 'trying to read missing data causes exception'() {
        given:
        DataResolver dataResolver = createDataResolver()
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)

        when:
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        and:
        dataInstance.inputStream

        then:
        FileNotFoundException ex = thrown()
        ex.message == "source/current/use_case/source_use_case.csv"
    }

    def 'writing and reading data works'() {
        given:
        DataResolver dataResolver = createDataResolver()
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)

        when:
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        and:
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(dataInstance.outputStream))
        out.println("Foo")
        out.close()
        and:
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInstance.inputStream))
        String line = reader.readLine()

        then:
        line == "Foo"
    }

    def 'written data can\'t be read after delete'() {
        given:
        DataResolver dataResolver = createDataResolver()
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)

        when:
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        and:
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(dataInstance.outputStream))
        out.println("Foo")
        out.close()
        and:
        boolean deleted = dataInstance.delete()
        and:
        dataInstance.inputStream

        then:
        deleted
        FileNotFoundException ex = thrown()
        ex.message == "source/current/use_case/source_use_case.csv"
    }

    def 'deleting missing instance returns false'() {
        given:
        DataResolver dataResolver = createDataResolver()
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)

        when:
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)

        then:
        !dataInstance.delete()
    }

    def 'path returns expected value'() {
        given:
        DataResolver dataResolver = createDataResolver()
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)

        when:
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)

        then:
        "source/current/use_case/source_use_case.csv" == dataInstance.getPath()
    }

    def "instance has expected capabilities"() {
        given:
        MemoryDataResolutionStrategy instance = new MemoryDataResolutionStrategy(new DefaultNamingStrategy())

        expect:
        instance.capabilities == DataResolutionCapability.ALL
    }

    def "instance supports expected capability #capability"() {
        given:
        MemoryDataResolutionStrategy instance = new MemoryDataResolutionStrategy(new DefaultNamingStrategy())

        expect:
        instance.supports(capability)

        where:
        capability << DataResolutionCapability.ALL
    }

    private static DataResolver createDataResolver() {
        return new DataResolver(new MemoryDataResolutionStrategy(new DefaultNamingStrategy()))
    }

}
