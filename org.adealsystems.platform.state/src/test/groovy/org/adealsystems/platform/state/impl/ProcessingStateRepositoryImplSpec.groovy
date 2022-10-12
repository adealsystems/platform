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

package org.adealsystems.platform.state.impl

import org.adealsystems.platform.id.DataFormat
import org.adealsystems.platform.id.DataIdentifier
import org.adealsystems.platform.id.DataInstance
import org.adealsystems.platform.id.DataResolver
import org.adealsystems.platform.id.DefaultNamingStrategy
import org.adealsystems.platform.id.memory.MemoryDataResolutionStrategy
import org.adealsystems.platform.state.ProcessingState
import spock.lang.Specification

class ProcessingStateRepositoryImplSpec extends Specification {

    def "reading missing returns expected value"() {
        given:
        MemoryDataResolutionStrategy dataResolutionStrategy = new MemoryDataResolutionStrategy(new DefaultNamingStrategy())
        DataResolver dataResolver = new DataResolver(dataResolutionStrategy)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        ProcessingStateRepositoryImpl instance = new ProcessingStateRepositoryImpl()

        when:
        Optional<ProcessingState> read = instance.getProcessingState(dataInstance)

        then:
        read == Optional.empty()
    }

    def "writing and reading works"() {
        given:
        MemoryDataResolutionStrategy dataResolutionStrategy = new MemoryDataResolutionStrategy(new DefaultNamingStrategy())
        DataResolver dataResolver = new DataResolver(dataResolutionStrategy)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        ProcessingStateRepositoryImpl instance = new ProcessingStateRepositoryImpl()

        ProcessingState state = new ProcessingState()
        state.errors = ["error1", "error2"]

        when:
        instance.setProcessingState(dataInstance, state)
        and:
        Optional<ProcessingState> read = instance.getProcessingState(dataInstance)
        println("data keys: " + dataResolutionStrategy.data.keySet())

        then:
        read == Optional.of(state)
    }

    def "writing, deleting and reading returns expected value"() {
        given:
        MemoryDataResolutionStrategy dataResolutionStrategy = new MemoryDataResolutionStrategy(new DefaultNamingStrategy())
        DataResolver dataResolver = new DataResolver(dataResolutionStrategy)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        ProcessingStateRepositoryImpl instance = new ProcessingStateRepositoryImpl()

        ProcessingState state = new ProcessingState()
        state.errors = ["error1", "error2"]

        when:
        instance.setProcessingState(dataInstance, state)
        and:
        instance.removeProcessingState(dataInstance)
        and:
        Optional<ProcessingState> read = instance.getProcessingState(dataInstance)

        then:
        read == Optional.empty()
    }

}
