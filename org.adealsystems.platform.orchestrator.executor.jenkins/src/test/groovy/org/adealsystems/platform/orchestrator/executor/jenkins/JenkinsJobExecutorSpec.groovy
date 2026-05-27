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

package org.adealsystems.platform.orchestrator.executor.jenkins

import org.adealsystems.platform.id.DataFormat
import org.adealsystems.platform.id.DataIdentifier
import org.adealsystems.platform.orchestrator.executor.CommandIdGenerator
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode
import spock.lang.Specification

class JenkinsJobExecutorSpec extends Specification {

    private static final DataIdentifier DATA_ID = new DataIdentifier('source', 'usecase', DataFormat.JSON)

    def 'single job executor returns undefined when no job is configured'() {
        given:
        def executor = new JenkinsJobExecutor(
            new EmptyJenkinsJobFactory(),
            Stub(CommandIdGenerator) {
                generate() >> 'command-1'
            }
        )

        when:
        def result = executor.execute(DATA_ID)

        then:
        result.result == ExecutorExitCode.UNDEFINED
        result.commandId == 'command-1'
        result.message == "No jenkins job is configured for $DATA_ID!"
    }

    def 'sequenced executor returns undefined when no data identifiers are specified'() {
        given:
        def executor = new JenkinsSequencedJobExecutor(
            new EmptyJenkinsJobFactory(),
            Stub(CommandIdGenerator)
        )

        when:
        def result = executor.execute(null as DataIdentifier[])

        then:
        result.result == ExecutorExitCode.UNDEFINED
        result.commandId == null
        result.message == 'No data identifiers specified!'
    }

    def 'special date executor returns undefined when no job is configured'() {
        given:
        def executor = new JenkinsSpecialDateSingleJobExecutor(
            new EmptyJenkinsSpecialDateJobFactory(),
            Stub(CommandIdGenerator) {
                generate() >> 'command-1'
            }
        )

        when:
        def result = executor.execute(java.time.LocalDate.of(2026, 5, 27), DATA_ID)

        then:
        result.result == ExecutorExitCode.UNDEFINED
        result.commandId == 'command-1'
        result.message == "No jenkins job is configured for $DATA_ID!"
    }

    private static class EmptyJenkinsJobFactory extends JenkinsJobFactory {
        EmptyJenkinsJobFactory() {
            super('https://jenkins.example', 'user', 'token')
        }

        @Override
        JenkinsJob getJob(DataIdentifier dataId) {
            null
        }
    }

    private static class EmptyJenkinsSpecialDateJobFactory extends JenkinsSpecialDateJobFactory {
        EmptyJenkinsSpecialDateJobFactory() {
            super('https://jenkins.example', 'user', 'token')
        }

        @Override
        JenkinsSpecialDateJob getJob(java.time.LocalDate inputDate, DataIdentifier dataId) {
            null
        }
    }
}
