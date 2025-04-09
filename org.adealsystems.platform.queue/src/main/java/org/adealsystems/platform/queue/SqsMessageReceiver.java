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

package org.adealsystems.platform.queue;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqsMessageReceiver implements MessageReceiver {
    private final SqsClient sqsClient;

    public SqsMessageReceiver(SqsClient sqsClient) {
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient must not be null!");
    }

    @Override
    public <T> List<T> receiveMessages(String queue, Function<String, T> convert) {
        Objects.requireNonNull(queue, "queue must not be null!");
        Objects.requireNonNull(convert, "convert function must not be null!");

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest
            .builder()
            .queueUrl(queue)
            .build();

        List<Message> messages;
        try {
            messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
        }
        catch (Exception ex) {
            throw new SqsReceiveMessagesException("Failed to receive messages from queue '" + queue + "'!", ex);
        }

        if (messages.isEmpty()) {
            return Collections.emptyList();
        }

        return messages
            .stream()
            .map(message -> {
                deleteMessage(queue, message);
                return convert.apply(message.body());
            })
            .collect(Collectors.toList());
    }

    private void deleteMessage(String queue, Message message) {
        DeleteMessageRequest request = DeleteMessageRequest
            .builder()
            .queueUrl(queue)
            .receiptHandle(message.receiptHandle())
            .build();

        try {
            sqsClient.deleteMessage(request);
        }
        catch (Exception ex) {
            throw new SqsDeleteMessageException("Failed to delete message " + message, ex);
        }
    }
}
