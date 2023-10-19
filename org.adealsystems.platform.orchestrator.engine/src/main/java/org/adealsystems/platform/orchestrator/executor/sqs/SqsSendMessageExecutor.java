/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator.executor.sqs;

import org.adealsystems.platform.orchestrator.AwsCredentialsOrchestrator;
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode;
import org.adealsystems.platform.orchestrator.executor.ExecutorResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.Map;
import java.util.Objects;

import static org.adealsystems.platform.orchestrator.executor.ExecutorExitCode.ERROR;
import static org.adealsystems.platform.orchestrator.executor.ExecutorExitCode.SUCCESS;


public class SqsSendMessageExecutor implements SendMessageExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqsSendMessageExecutor.class);

    private final Map<String, String> queueNameMap;
    private final AwsCredentialsOrchestrator awsCredentialsOrchestrator;

    public SqsSendMessageExecutor(AwsCredentialsOrchestrator awsCredentialsOrchestrator, Map<String, String> queueNameMap) {
        this.awsCredentialsOrchestrator = Objects.requireNonNull(awsCredentialsOrchestrator, "awsCredentialsOrchestrator must not be null!");
        this.queueNameMap = Objects.requireNonNull(queueNameMap, "queueNameMap must not be null!");
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendMessage(String queue, String message) {
        SendMessageResponse response;
        String queueName = queueNameMap.get(queue);
        try (SqsClient sqsClient = getSqsClient()) {
            SendMessageRequest messageRequest = SendMessageRequest.builder()
                .queueUrl(queueName)
                .messageBody(message)
                .build();

            response = sqsClient.sendMessage(messageRequest);
            return new ExecutorResult<>(SUCCESS, queueName, response.toString());
        } catch (SqsException ex) {
            LOGGER.error("AwsSqsSendMessageExecutor failed. Error writing message body {}!", message, ex);
            return new ExecutorResult<>(ERROR, queueName, message);
        }
    }

    public SqsClient getSqsClient() {
        return SqsClient.builder()
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
