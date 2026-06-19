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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class S3EventReceiverRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3EventReceiverRunnable.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String queueName;

    private final SqsClient sqsClient;

    private final InternalEventSender eventSender;

    public S3EventReceiverRunnable(String queueName, SqsClient sqsClient, InternalEventSender eventSender) {
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
                try {
                    List<InternalEvent> events = convertNotification(messageBody);
                    for (InternalEvent event : events) {
                        LOGGER.info("About to send event {}", event);
                        try {
                            eventSender.sendEvent(event);
                        } catch (Exception ex) {
                            LOGGER.error("Error sending event {}!", event, ex);
                        }
                    }
                } catch (Exception ex) {
                    LOGGER.error("Error reading message body {}!", messageBody, ex);
                    deleteMessage(message);
                    continue;
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

    static List<InternalEvent> convertNotification(String messageBody) throws Exception {
        JsonNode root = OBJECT_MAPPER.readTree(messageBody);
        JsonNode records = root.get("Records");
        if (records == null || !records.isArray()) {
            return Collections.emptyList();
        }

        List<InternalEvent> result = new ArrayList<>();
        for (JsonNode record : records) {
            if (record == null || record.isNull()) {
                continue;
            }

            InternalEvent event = convertRecord(record);
            if (event == null) {
                LOGGER.warn("Failed to convert record {}!", record);
                continue;
            }

            result.add(event);
        }

        return result;
    }

    private static InternalEvent convertRecord(JsonNode record) {
        JsonNode s3Entity = record.get("s3");
        if (s3Entity == null || s3Entity.isNull()) {
            return null;
        }

        JsonNode bucket = s3Entity.get("bucket");
        if (bucket == null || bucket.isNull()) {
            return null;
        }

        JsonNode object = s3Entity.get("object");
        if (object == null || object.isNull()) {
            return null;
        }

        String objectKey = getText(object, "key");
        if (objectKey == null) {
            return null;
        }
        String eventId = URLDecoder.decode(objectKey, StandardCharsets.ISO_8859_1);

        InternalEvent event = new InternalEvent();
        event.setType(InternalEventType.FILE);
        event.setId(eventId);
        event.setTimestamp(LocalDateTime.now(ZoneId.systemDefault()));
        event.setAttributeValue(S3Constants.BUCKET_NAME, getText(bucket, "name"));
        event.setAttributeValue(S3Constants.FILE_OPERATION, getText(record, "eventName"));

        JsonNode fileSize = object.get("size");
        if (fileSize != null && fileSize.canConvertToLong()) {
            event.setAttributeValue(S3Constants.FILE_SIZE, String.valueOf(fileSize.longValue()));
        }

        String eventTimestamp = getText(record, "eventTime");
        if (eventTimestamp != null) {
            event.setAttributeValue(S3Constants.EVENT_TIMESTAMP, eventTimestamp);
        }

        return event;
    }

    private static String getText(JsonNode node, String fieldName) {
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) {
            return null;
        }
        return child.asText();
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
