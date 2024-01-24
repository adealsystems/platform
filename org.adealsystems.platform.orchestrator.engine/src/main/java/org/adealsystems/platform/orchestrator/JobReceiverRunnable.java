/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.orchestrator.executor.MultipleJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JobReceiverRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobReceiverRunnable.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String queueName;

    private final SqsClient sqsClient;

    private final MultipleJobExecutor asyncJobExecutor;

    public JobReceiverRunnable(
        String queueName,
        SqsClient sqsClient,
        MultipleJobExecutor asyncJobExecutor
    ) {
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null!");
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient must not be null!");
        this.asyncJobExecutor = Objects.requireNonNull(asyncJobExecutor, "asyncJobExecutor must not be null!");
    }

    @Override
    public void run() {
        LOGGER.info("Starting processor job event receiver thread on queue '{}'", queueName);
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
            .queueUrl(queueName)
            .build();

        // should be started as daemon thread
        List<JobMessage> jobs = new ArrayList<>();

        while (true) {
            List<Message> messages;
            try {
                messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            } catch (Exception ex) {
                LOGGER.error("Failed to receive messages!", ex);
                continue;
            }

            if ((messages.isEmpty() && !jobs.isEmpty()) || jobs.size() >= 20) {
                LOGGER.info("Executing {} collected jobs", jobs.size());
                executeJobs(jobs);
                jobs.clear();
            }

            if (messages.isEmpty()) {
                try {
                    sleep(60_000);
                } catch (InterruptedException ex) {
                    break;
                }

                continue;
            }

            // Read all available requests
            for (Message message : messages) {
                LOGGER.debug("Processing message {}", message);

                String messageBody = message.body();
                JobMessage job;
                try {
                    job = OBJECT_MAPPER.readValue(messageBody, JobMessage.class);
                } catch (JsonProcessingException ex) {
                    LOGGER.error("Error reading message body {}!", messageBody, ex);
                    deleteMessage(message);
                    continue;
                }

                jobs.add(job);

                deleteMessage(message);
            }

            try {
                sleep(10);
            } catch (InterruptedException ex) {
                break;
            }
        }
    }

    private void executeJobs(List<JobMessage> jobs) {
        LOGGER.debug("Executing job bundle: {}", jobs);
        DataIdentifier[] dataIds = new DataIdentifier[jobs.size()];
        for (int i = 0; i < jobs.size(); i++) {
            String dataId = jobs.get(i).getDataIdentifier();
            dataIds[i] = DataIdentifier.fromString(dataId);
        }

        asyncJobExecutor.execute(dataIds);
    }

    private void sleep(long value) throws InterruptedException {
        Thread.sleep(value);
    }

    private void deleteMessage(Message message) {
        DeleteMessageRequest request = DeleteMessageRequest.builder()
            .queueUrl(queueName)
            .receiptHandle(message.receiptHandle())
            .build();

        try {
            sqsClient.deleteMessage(request);
        } catch (Exception ex) {
            LOGGER.error("Failed to delete message {}", message, ex);
        }
    }
}
