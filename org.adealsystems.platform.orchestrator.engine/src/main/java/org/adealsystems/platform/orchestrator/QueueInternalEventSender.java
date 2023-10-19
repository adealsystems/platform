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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Map;
import java.util.Objects;

public class QueueInternalEventSender implements InternalEventSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueInternalEventSender.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String queueName;

    private final SqsClient sqsClient;

    public QueueInternalEventSender(String queueName, SqsClient sqsClient) {
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null!");
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient must not be null!");
    }

    @Override
    public boolean isEmpty() {
        GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
            .queueUrl(queueName)
            .build();
        GetQueueAttributesResponse response = sqsClient.getQueueAttributes(request);
        Map<QueueAttributeName, String> attributes = response.attributes();
        try {
            int countMessages = Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
            int countMessagesDelayed = Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED));
            int countMessagesNotVisible = Integer.parseInt(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE));
            return countMessages == 0 && countMessagesDelayed == 0 && countMessagesNotVisible == 0;
        } catch (NumberFormatException ex) {
            LOGGER.warn("Unable to retrieve queue attributes!", ex);
            return false;
        }
    }

    @Override
    public void sendEvent(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");

        try {
            String serialized = OBJECT_MAPPER.writeValueAsString(event);
            LOGGER.info("Sending internal event for {}", serialized);
            SendMessageRequest message = SendMessageRequest.builder()
                .queueUrl(queueName)
                .messageBody(serialized)
                .build();

            sqsClient.sendMessage(message);
        } catch (JsonProcessingException ex) {
            LOGGER.error("Error writing message body {}!", event, ex);
        }
    }
}
