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
import org.adealsystems.platform.id.DataIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.Objects;

public class QueueJobRegistrar implements JobRegistrar {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueJobRegistrar.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String queueName;

    private final SqsClient sqsClient;

    public QueueJobRegistrar(String queueName, SqsClient sqsClient) {
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null!");
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient must not be null!");
    }

    @Override
    public void registerJob(DataIdentifier dataIdentifier, boolean failOnError) {
        Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");

        JobMessage procMessage = new JobMessage();
        procMessage.setDataIdentifier(dataIdentifier.toString());

        try {
            String serialized = OBJECT_MAPPER.writeValueAsString(procMessage);
            LOGGER.info("Sending processor request message for {}", serialized);
            SendMessageRequest message = SendMessageRequest.builder()
                .queueUrl(queueName)
                .messageBody(serialized)
                .build();

            sqsClient.sendMessage(message);
        } catch (JsonProcessingException ex) {
            LOGGER.error("Error writing message body {}!", procMessage, ex);
        }
    }
}
