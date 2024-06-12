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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JobReceiverRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobReceiverRunnable.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int DEFAULT_WAITING_INTERVAL = 10_000;
    private static final int DEFAULT_JOB_BUNDLE_SIZE = 20;

    private final SqsClient sqsClient;

    private final Map<String, MultipleJobExecutor> asyncJobExecutors;

    private int waitingInterval = DEFAULT_WAITING_INTERVAL;
    private int jobBundleSize = DEFAULT_JOB_BUNDLE_SIZE;

    public JobReceiverRunnable(
        SqsClient sqsClient,
        Map<String, MultipleJobExecutor> asyncJobExecutors
    ) {
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient must not be null!");
        this.asyncJobExecutors = Objects.requireNonNull(asyncJobExecutors, "asyncJobExecutors must not be null!");
    }

    public void setWaitingInterval(int waitingInterval) {
        this.waitingInterval = waitingInterval;
    }

    public void setJobBundleSize(int jobBundleSize) {
        this.jobBundleSize = jobBundleSize;
    }

    @Override
    public void run() {
        LOGGER.info("Starting async job event receiver thread");

        // should be started as daemon thread
        Map<String, List<JobMessage>> allJobs = new HashMap<>();

        while (true) {
            boolean waitingMode = true;

            for (Map.Entry<String, MultipleJobExecutor> entry : asyncJobExecutors.entrySet()) {
                String queueName = entry.getKey();
                MultipleJobExecutor jobExecutor = entry.getValue();
                List<JobMessage> jobs = allJobs.get(queueName);
                if (jobs == null) {
                    jobs = new ArrayList<>(); // NOPMD
                    allJobs.put(queueName, jobs);
                }

                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest
                    .builder()
                    .queueUrl(queueName)
                    .build();

                List<Message> messages;
                try {
                    messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
                }
                catch (Exception ex) {
                    LOGGER.error("Failed to receive messages from queue " + queueName + "!", ex);
                    continue;
                }

                if ((messages.isEmpty() && !jobs.isEmpty()) || jobs.size() >= jobBundleSize) {
                    // No new messages, but collected job requests
                    // OR more than <jobBundleSize> collected job requests
                    LOGGER.info("Executing {} collected jobs from {}", jobs.size(), queueName);
                    executeJobs(jobs, jobExecutor);
                    jobs.clear();
                }

                // Read all available requests
                if (!messages.isEmpty()) {
                    waitingMode = false;
                    for (Message message : messages) {
                        LOGGER.debug("Processing message {}", message);

                        String messageBody = message.body();
                        JobMessage job;
                        try {
                            job = OBJECT_MAPPER.readValue(messageBody, JobMessage.class);
                        }
                        catch (JsonProcessingException ex) {
                            LOGGER.error("Error reading message body {}!", messageBody, ex);
                            continue;
                        }
                        finally {
                            deleteMessage(message, queueName);
                        }

                        jobs.add(job);
                    }
                }
            }

            // delay before next check
            try {
                if (waitingMode) {
                    sleep(waitingInterval);
                }
                else {
                    // sleep short
                    sleep(100);
                }
            }
            catch (InterruptedException ex) {
                break;
            }
        }
    }

    private void executeJobs(List<JobMessage> jobs, MultipleJobExecutor jobExecutor) {
        LOGGER.debug("Executing job bundle: {}", jobs);
        DataIdentifier[] dataIds = new DataIdentifier[jobs.size()];
        for (int i = 0; i < jobs.size(); i++) {
            String dataId = jobs.get(i).getDataIdentifier();
            dataIds[i] = DataIdentifier.fromString(dataId);
        }

        jobExecutor.execute(dataIds);
    }

    private void sleep(long value) throws InterruptedException {
        Thread.sleep(value);
    }

    private void deleteMessage(Message message, String queueName) {
        DeleteMessageRequest request = DeleteMessageRequest
            .builder()
            .queueUrl(queueName)
            .receiptHandle(message.receiptHandle())
            .build();

        try {
            sqsClient.deleteMessage(request);
        }
        catch (Exception ex) {
            LOGGER.error("Failed to delete message {}", message, ex);
        }
    }
}
