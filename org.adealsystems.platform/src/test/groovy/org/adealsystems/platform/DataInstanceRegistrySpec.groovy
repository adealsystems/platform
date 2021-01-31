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

import java.time.LocalDate

import org.adealsystems.platform.DataFormat
import org.adealsystems.platform.DataIdentifier
import org.adealsystems.platform.DataInstanceRegistry
import org.adealsystems.platform.DataResolver
import org.adealsystems.platform.DefaultNamingStrategy
import org.adealsystems.platform.exceptions.DuplicateInstanceRegistrationException
import org.adealsystems.platform.exceptions.DuplicateUniqueIdentifierException
import org.adealsystems.platform.memory.MemoryDataResolutionStrategy

class DataInstanceRegistrySpec extends Specification {
    def "expected exception is thrown"() {
        given:
        def instance = new DataInstanceRegistry()
        def dataResolver = createDataResolver()
        def dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        def dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        and:
        instance.register(dataInstance)

        when:
        instance.register(dataInstance)

        then:
        DuplicateInstanceRegistrationException ex = thrown()
        ex.getDataInstance() == dataInstance
        ex.message == "dataInstance source:use_case:CSV_SEMICOLON#current is already registered!"
    }

    def "resolveAll works"() {
        given:
        def instance = new DataInstanceRegistry()
        def dataResolver = createDataResolver()
        def dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        def currentDataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        def dateDataInstance = dataResolver.createDateInstance(dataIdentifier, LocalDate.of(2020, 5, 10))

        and:
        instance.register(currentDataInstance)
        instance.register(dateDataInstance)

        when:
        def resolved = instance.resolveAll(dataIdentifier)

        then:
        resolved == [currentDataInstance, dateDataInstance] as Set
    }

    def "resolveUnique throws exception in case of multiple entries"() {
        given:
        def instance = new DataInstanceRegistry()
        def dataResolver = createDataResolver()
        def dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        def currentDataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        def dateDataInstance = dataResolver.createDateInstance(dataIdentifier, LocalDate.of(2020, 5, 10))

        and:
        instance.register(currentDataInstance)
        instance.register(dateDataInstance)

        when:
        instance.resolveUnique(dataIdentifier)

        then:
        DuplicateUniqueIdentifierException ex = thrown()
        ex.getDataIdentifier() == dataIdentifier
        ex.message == "Multiple data instances for dataIdentifier source:use_case:CSV_SEMICOLON!"
    }

    def "resolveUnique works with current"() {
        given:
        def instance = new DataInstanceRegistry()
        def dataResolver = createDataResolver()
        def dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        def currentDataInstance = dataResolver.createCurrentInstance(dataIdentifier)

        and:
        instance.register(currentDataInstance)

        when:
        def result = instance.resolveUnique(dataIdentifier)

        then:
        result == Optional.of(currentDataInstance)
    }

    def "resolveUnique works with date"() {
        given:
        def instance = new DataInstanceRegistry()
        def dataResolver = createDataResolver()
        def dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        def dateDataInstance = dataResolver.createDateInstance(dataIdentifier, LocalDate.of(2020, 5, 10))

        and:
        instance.register(dateDataInstance)

        when:
        def result = instance.resolveUnique(dataIdentifier)

        then:
        result == Optional.of(dateDataInstance)
    }

    def "resolveUnique returns empty Optional for unregistered identifier"() {
        given:
        def instance = new DataInstanceRegistry()
        def dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)

        when:
        def result = instance.resolveUnique(dataIdentifier)

        then:
        result == Optional.empty()
    }

    private static DataResolver createDataResolver() {
        return new DataResolver(new MemoryDataResolutionStrategy(new DefaultNamingStrategy()))
    }
}
