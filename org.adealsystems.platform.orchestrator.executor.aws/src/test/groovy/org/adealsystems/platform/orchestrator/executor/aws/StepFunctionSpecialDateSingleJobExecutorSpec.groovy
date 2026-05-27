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

import java.time.LocalDate

class StepFunctionSpecialDateSingleJobExecutorSpec extends Specification {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
    private static final String STATE_MACHINE_ARN = 'arn:aws:states:eu-central-1:123456789012:stateMachine:test'
    private static final DataIdentifier DATA_ID = new DataIdentifier('source', 'usecase', DataFormat.JSON)
    private static final AwsCredentialsOrchestrator CREDENTIALS = new AwsCredentialsOrchestrator('access', 'secret', 'eu-central-1')

    def 'special date executor forwards input date through json input'() {
        given:
        def requests = []
        def client = Mock(SfnClient)
        def executor = new TestStepFunctionSpecialDateSingleJobExecutor(
            new TestStepFunctionSpecialDateJobFactory(),
            Stub(CommandIdGenerator) {
                generate() >> 'special-1'
            },
            CREDENTIALS,
            client
        )

        when:
        def result = executor.execute(LocalDate.of(2026, 5, 27), DATA_ID)

        then:
        1 * client.startExecution(_ as StartExecutionRequest) >> { StartExecutionRequest request ->
            requests << request
            StartExecutionResponse.builder().executionArn('execution').build()
        }
        1 * client.close()
        result.result == ExecutorExitCode.SUCCESS
        result.commandId == 'special-1'

        and:
        requests.size() == 1
        requests[0].stateMachineArn() == STATE_MACHINE_ARN
        def payload = OBJECT_MAPPER.readValue(requests[0].input(), Map)
        payload.INPUTDATE == '2026-05-27'
        payload.job == 'source:usecase:JSON'
        payload.commandId == 'special-1'
    }

    private static class TestStepFunctionSpecialDateSingleJobExecutor extends StepFunctionSpecialDateSingleJobExecutor {
        private final SfnClient sfnClient

        TestStepFunctionSpecialDateSingleJobExecutor(
            StepFunctionSpecialDateJobFactory jobFactory,
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

    private static class TestStepFunctionSpecialDateJobFactory extends StepFunctionSpecialDateJobFactory {
        @Override
        StepFunctionSpecialDateJob getJob(LocalDate inputDate, DataIdentifier dataId) {
            new TestStepFunctionSpecialDateJob()
        }
    }

    private static class TestStepFunctionSpecialDateJob extends StepFunctionSpecialDateJob {
        TestStepFunctionSpecialDateJob() {
            super(STATE_MACHINE_ARN)
        }

        @Override
        protected Map<String, ?> prepareInputParameters(LocalDate inputDate, DataIdentifier dataIdentifier) {
            [
                INPUTDATE: inputDate.toString(),
                job      : dataIdentifier.toString()
            ]
        }
    }
}
