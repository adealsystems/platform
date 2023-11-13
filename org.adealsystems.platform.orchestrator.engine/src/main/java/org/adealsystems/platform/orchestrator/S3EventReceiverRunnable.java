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

package org.adealsystems.platform.orchestrator;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.UnsupportedEncodingException;
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
                S3EventNotification s3EventNotification;
                try {
                    s3EventNotification = OBJECT_MAPPER.readValue(messageBody, S3EventNotification.class);
                } catch (JsonProcessingException ex) {
                    LOGGER.error("Error reading message body {}!", messageBody, ex);
                    deleteMessage(message);
                    continue;
                }

                List<InternalEvent> events = convertNotification(s3EventNotification);
                for (InternalEvent event : events) {
                    LOGGER.info("About to send event {}", event);
                    try {
                        eventSender.sendEvent(event);
                    } catch (Exception ex) {
                        LOGGER.error("Error sending event {}!", event, ex);
                    }
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

    static List<InternalEvent> convertNotification(S3EventNotification notification) {
        List<S3EventNotification.S3EventNotificationRecord> records = notification.getRecords();
        if (records == null) {
            return Collections.emptyList();
        }

        List<InternalEvent> result = new ArrayList<>();
        for (S3EventNotification.S3EventNotificationRecord record : records) {
            if (record == null) {
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

    private static InternalEvent convertRecord(S3EventNotification.S3EventNotificationRecord record) {
        S3EventNotification.S3Entity s3Entity = record.getS3();
        if (s3Entity == null) {
            return null;
        }

        S3EventNotification.S3BucketEntity bucket = s3Entity.getBucket();
        if (bucket == null) {
            return null;
        }

        S3EventNotification.S3ObjectEntity object = s3Entity.getObject();
        if (object == null) {
            return null;
        }

        String eventId;
        try {
            eventId = URLDecoder.decode(object.getKey(), StandardCharsets.ISO_8859_1.toString());
        } catch (UnsupportedEncodingException ex) {
            eventId = object.getKey();
            LOGGER.warn("Unable to decode eventId {}", eventId, ex);
        }
        InternalEvent event = new InternalEvent();
        event.setType(InternalEventType.FILE);
        event.setId(eventId);
        event.setTimestamp(LocalDateTime.now(ZoneId.systemDefault()));
        event.setAttributeValue(S3Constants.BUCKET_NAME, bucket.getName());
        event.setAttributeValue(S3Constants.FILE_OPERATION, record.getEventName());

        Long fileSize = object.getSizeAsLong();
        if (fileSize != null) {
            event.setAttributeValue(S3Constants.FILE_SIZE, String.valueOf(fileSize));
        }

        DateTime eventTimestamp = record.getEventTime();
        if (eventTimestamp != null) {
            event.setAttributeValue(S3Constants.EVENT_TIMESTAMP, eventTimestamp.toString());
        }

        return event;
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
