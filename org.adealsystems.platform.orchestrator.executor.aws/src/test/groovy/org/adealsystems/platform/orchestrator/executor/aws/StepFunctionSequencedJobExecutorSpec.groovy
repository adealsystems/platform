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

package org.adealsystems.platform.orchestrator.executor.aws

import com.fasterxml.jackson.databind.ObjectMapper
import org.adealsystems.platform.id.DataFormat
import org.adealsystems.platform.id.DataIdentifier
import org.adealsystems.platform.orchestrator.AwsCredentialsOrchestrator
import org.adealsystems.platform.orchestrator.executor.CommandIdGenerator
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode
import software.amazon.awssdk.services.sfn.SfnClient
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse
import spock.lang.Specification

class StepFunctionSequencedJobExecutorSpec extends Specification {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
    private static final String STATE_MACHINE_ARN = 'arn:aws:states:eu-central-1:123456789012:stateMachine:test'
    private static final DataIdentifier FIRST_DATA_ID = new DataIdentifier('source', 'first', DataFormat.JSON)
    private static final DataIdentifier SECOND_DATA_ID = new DataIdentifier('source', 'second', DataFormat.JSON)
    private static final AwsCredentialsOrchestrator CREDENTIALS = new AwsCredentialsOrchestrator('access', 'secret', 'eu-central-1')

    def 'sequenced executor starts all jobs with the same command id'() {
        given:
        def requests = []
        def client = Mock(SfnClient)
        def executor = new TestStepFunctionSequencedJobExecutor(
            new TestStepFunctionJobFactory(),
            Stub(CommandIdGenerator) {
                generate() >> 'sequence-1'
            },
            CREDENTIALS,
            client
        )

        when:
        def result = executor.execute(FIRST_DATA_ID, SECOND_DATA_ID)

        then:
        2 * client.startExecution(_ as StartExecutionRequest) >> { StartExecutionRequest request ->
            requests << request
            StartExecutionResponse.builder().executionArn('execution').build()
        }
        2 * client.close()
        result.result == ExecutorExitCode.SUCCESS
        result.commandId == 'sequence-1'

        and:
        requests.size() == 2
        def firstPayload = OBJECT_MAPPER.readValue(requests[0].input(), Map)
        firstPayload.use_case == 'first'
        firstPayload.commandId == 'sequence-1'
        def secondPayload = OBJECT_MAPPER.readValue(requests[1].input(), Map)
        secondPayload.use_case == 'second'
        secondPayload.commandId == 'sequence-1'
    }

    private static class TestStepFunctionSequencedJobExecutor extends StepFunctionSequencedJobExecutor {
        private final SfnClient sfnClient

        TestStepFunctionSequencedJobExecutor(
            StepFunctionJobFactory jobFactory,
            CommandIdGenerator commandIdGenerator,
            AwsCredentialsOrchestrator awsCredentialsOrchestrator,
            SfnClient sfnClient
        ) {
            super(jobFactory, commandIdGenerator, awsCredentialsOrchestrator)
            this.sfnClient = sfnClient
        }

        @Override
        SfnClient getSfnClient() {
            sfnClient
        }
    }

    private static class TestStepFunctionJobFactory extends StepFunctionJobFactory {
        @Override
        StepFunctionJob getJob(DataIdentifier dataId) {
            new TestStepFunctionJob(dataId.useCase)
        }
    }

    private static class TestStepFunctionJob extends StepFunctionJob {
        private final String useCase

        TestStepFunctionJob(String useCase) {
            super(STATE_MACHINE_ARN)
            this.useCase = useCase
        }

        @Override
        protected Map<String, ?> prepareInputParameters(DataIdentifier dataIdentifier) {
            [
                use_case: useCase
            ]
        }
    }
}
