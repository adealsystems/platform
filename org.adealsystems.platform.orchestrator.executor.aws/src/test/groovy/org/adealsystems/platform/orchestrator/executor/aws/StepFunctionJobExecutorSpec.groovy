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

class StepFunctionJobExecutorSpec extends Specification {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
    private static final String STATE_MACHINE_ARN = 'arn:aws:states:eu-central-1:123456789012:stateMachine:test'
    private static final String EXECUTION_ARN = 'arn:aws:states:eu-central-1:123456789012:execution:test:execution'
    private static final DataIdentifier DATA_ID = new DataIdentifier('source', 'usecase', DataFormat.JSON)
    private static final AwsCredentialsOrchestrator CREDENTIALS = new AwsCredentialsOrchestrator('access', 'secret', 'eu-central-1')

    def 'single job executor starts step function with expected json input'() {
        given:
        def requests = []
        def client = Mock(SfnClient)
        def executor = new TestStepFunctionJobExecutor(
            new TestStepFunctionJobFactory(new TestStepFunctionJob()),
            Stub(CommandIdGenerator) {
                generate() >> 'command-1'
            },
            CREDENTIALS,
            client
        )

        when:
        def result = executor.execute(DATA_ID)

        then:
        1 * client.startExecution(_ as StartExecutionRequest) >> { StartExecutionRequest request ->
            requests << request
            StartExecutionResponse.builder().executionArn(EXECUTION_ARN).build()
        }
        1 * client.close()
        result.result == ExecutorExitCode.SUCCESS
        result.commandId == 'command-1'
        result.message == "Execution ARN: $EXECUTION_ARN"

        and:
        requests.size() == 1
        requests[0].stateMachineArn() == STATE_MACHINE_ARN
        def payload = OBJECT_MAPPER.readValue(requests[0].input(), Map)
        payload.task_version == '1.2.0'
        payload.jar_version == '2.5.1'
        payload.additional_properties.triggered_by == 'orchestrator'
        payload.commandId == 'command-1'
    }

    def 'single job executor returns undefined when no job is configured'() {
        given:
        def client = Mock(SfnClient)
        def executor = new TestStepFunctionJobExecutor(
            new TestStepFunctionJobFactory(null),
            Stub(CommandIdGenerator) {
                generate() >> 'command-1'
            },
            CREDENTIALS,
            client
        )

        when:
        def result = executor.execute(DATA_ID)

        then:
        0 * client._
        result.result == ExecutorExitCode.UNDEFINED
        result.commandId == 'command-1'
        result.message == "No step-function job is configured for $DATA_ID!"
    }

    private static class TestStepFunctionJobExecutor extends StepFunctionJobExecutor {
        private final SfnClient sfnClient

        TestStepFunctionJobExecutor(
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
        private final StepFunctionJob job

        TestStepFunctionJobFactory(StepFunctionJob job) {
            this.job = job
        }

        @Override
        StepFunctionJob getJob(DataIdentifier dataId) {
            job
        }
    }

    private static class TestStepFunctionJob extends StepFunctionJob {
        TestStepFunctionJob() {
            super(STATE_MACHINE_ARN)
        }

        @Override
        protected Map<String, ?> prepareInputParameters(DataIdentifier dataIdentifier) {
            [
                task_version         : '1.2.0',
                jar_version          : '2.5.1',
                additional_properties: [
                    triggered_by: 'orchestrator'
                ]
            ]
        }
    }
}
