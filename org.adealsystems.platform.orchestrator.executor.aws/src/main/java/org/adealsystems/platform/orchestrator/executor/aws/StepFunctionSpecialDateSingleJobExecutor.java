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

package org.adealsystems.platform.orchestrator.executor.aws;

import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.orchestrator.AwsCredentialsOrchestrator;
import org.adealsystems.platform.orchestrator.executor.CommandIdGenerator;
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode;
import org.adealsystems.platform.orchestrator.executor.ExecutorResult;
import org.adealsystems.platform.orchestrator.executor.SpecialDateSingleJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.SfnException;
import software.amazon.awssdk.services.sfn.model.StartExecutionRequest;
import software.amazon.awssdk.services.sfn.model.StartExecutionResponse;

import java.time.LocalDate;
import java.util.Objects;

import static org.adealsystems.platform.orchestrator.executor.ExecutorExitCode.ERROR;
import static org.adealsystems.platform.orchestrator.executor.ExecutorExitCode.SUCCESS;
import static org.adealsystems.platform.orchestrator.executor.ExecutorExitCode.UNDEFINED;

public class StepFunctionSpecialDateSingleJobExecutor implements SpecialDateSingleJobExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(StepFunctionSpecialDateSingleJobExecutor.class);

    private final CommandIdGenerator commandIdGenerator;

    private final StepFunctionSpecialDateJobFactory jobFactory;

    private final AwsCredentialsOrchestrator awsCredentialsOrchestrator;

    public StepFunctionSpecialDateSingleJobExecutor(
        StepFunctionSpecialDateJobFactory jobFactory,
        CommandIdGenerator commandIdGenerator,
        AwsCredentialsOrchestrator awsCredentialsOrchestrator
    ) {
        this.jobFactory = Objects.requireNonNull(jobFactory, "jobFactory must not be null!");
        this.commandIdGenerator = Objects.requireNonNull(commandIdGenerator, "commandIdGenerator must not be null!");
        this.awsCredentialsOrchestrator = Objects.requireNonNull(awsCredentialsOrchestrator, "awsCredentialsOrchestrator must not be null!");
    }

    @Override
    public ExecutorResult<ExecutorExitCode> execute(LocalDate inputDate, DataIdentifier dataId) {
        String commandId = commandIdGenerator.generate();

        StepFunctionSpecialDateJob job = jobFactory.getJob(inputDate, dataId);
        if (job == null) {
            return new ExecutorResult<>(UNDEFINED, commandId, "No step-function job is configured for " + dataId + "!");
        }

        String input = job.createInput(commandId, inputDate, dataId);
        StartExecutionRequest request = StartExecutionRequest.builder()
            .stateMachineArn(job.getStateMachineArn())
            .input(input)
            .build();

        try (SfnClient sfnClient = getSfnClient()) {
            StartExecutionResponse response = sfnClient.startExecution(request);
            return new ExecutorResult<>(SUCCESS, commandId, "Execution ARN: " + response.executionArn());
        } catch (SfnException ex) {
            LOGGER.error("StepFunctionSpecialDateSingleJobExecutor failed for state machine {}!", job.getStateMachineArn(), ex);
            return new ExecutorResult<>(ERROR, commandId, ex.awsErrorDetails().errorMessage(), ex);
        } catch (RuntimeException ex) {
            LOGGER.error("StepFunctionSpecialDateSingleJobExecutor failed for state machine {}!", job.getStateMachineArn(), ex);
            return new ExecutorResult<>(ERROR, commandId, "Unable to start Step Functions execution!", ex);
        }
    }

    public SfnClient getSfnClient() {
        return SfnClient.builder()
            .region(Region.of(awsCredentialsOrchestrator.getAwsRegion()))
            .credentialsProvider(() -> new AwsCredentials() {
                @Override
                public String accessKeyId() {
                    return awsCredentialsOrchestrator.getAwsAccessKey();
                }

                @Override
                public String secretAccessKey() {
                    return awsCredentialsOrchestrator.getAwsSecretKey();
                }
            })
            .build();
    }
}
