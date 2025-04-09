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

package org.adealsystems.platform.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
import java.util.Objects;

public class InternalEventReceiverRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InternalEventReceiverRunnable.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String queueName;

    private final SqsClient sqsClient;

    private final InternalEventSender eventSender;

    public InternalEventReceiverRunnable(String queueName, SqsClient sqsClient, InternalEventSender eventSender) {
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null!");
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient must not be null!");
        this.eventSender = Objects.requireNonNull(eventSender, "eventSender must not be null!");
    }

    @Override
    public void run() {
        LOGGER.info("Starting SQS event receiver thread on queue '{}'", queueName);
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
            .queueUrl(queueName)
            .build();

        // should be started as daemon thread
        while (true) {
            List<Message> messages;
            try {
                messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            } catch (Exception ex) {
                LOGGER.error("Failed to receive messages!", ex);
                continue;
            }

            if (messages.isEmpty()) {
                try {
                    sleep(5_000);
                } catch (InterruptedException ex) {
                    break;
                }

                continue;
            }

            for (Message message : messages) {
                LOGGER.debug("Processing message {}", message);

                String messageBody = message.body();
                InternalEvent event;
                try {
                    event = OBJECT_MAPPER.readValue(messageBody, InternalEvent.class);
                } catch (JsonProcessingException ex) {
                    LOGGER.error("Error reading message body {}!", messageBody, ex);
                    deleteMessage(message);
                    continue;
                }

                LOGGER.info("About to send event {}", event);
                try {
                    eventSender.sendEvent(event);
                } catch (Exception ex) {
                    LOGGER.error("Error sending event {}!", event, ex);
                }

                deleteMessage(message);
            }

            try {
                sleep(100);
            } catch (InterruptedException ex) {
                break;
            }
        }
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
