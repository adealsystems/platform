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

package org.adealsystems.platform.state.impl

import org.adealsystems.platform.id.DataFormat
import org.adealsystems.platform.id.DataIdentifier
import org.adealsystems.platform.id.DataInstance
import org.adealsystems.platform.id.DataResolver
import org.adealsystems.platform.id.DefaultNamingStrategy
import org.adealsystems.platform.id.file.FileDataResolutionStrategy
import org.adealsystems.platform.state.ProcessingState
import spock.lang.Specification
import spock.lang.TempDir

class ProcessingStateRepositoryImplSpec extends Specification {

    @TempDir
    File tempDir

    def "reading missing returns expected value"() {
        given:
        FileDataResolutionStrategy dataResolutionStrategy = new FileDataResolutionStrategy(new DefaultNamingStrategy(), tempDir)
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
        FileDataResolutionStrategy dataResolutionStrategy = new FileDataResolutionStrategy(new DefaultNamingStrategy(), tempDir)
        DataResolver dataResolver = new DataResolver(dataResolutionStrategy)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        ProcessingStateRepositoryImpl instance = new ProcessingStateRepositoryImpl()

        ProcessingState state = new ProcessingState()
        state.errors = ["error1", "error2"]

        when:
        instance.setProcessingState(dataInstance, state)
        and: 'writing twice works'
        instance.setProcessingState(dataInstance, state)
        and:
        Optional<ProcessingState> read = instance.getProcessingState(dataInstance)

        then:
        read == Optional.of(state)
    }

    def "writing, deleting and reading returns expected value"() {
        given:
        FileDataResolutionStrategy dataResolutionStrategy = new FileDataResolutionStrategy(new DefaultNamingStrategy(), tempDir)
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
