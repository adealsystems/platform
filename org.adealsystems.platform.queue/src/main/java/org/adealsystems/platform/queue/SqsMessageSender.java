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

package org.adealsystems.platform.queue;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.Objects;

public class SqsMessageSender implements MessageSender {
    private final SqsClient sqsClient;

    public SqsMessageSender(SqsClient sqsClient) {
        this.sqsClient = Objects.requireNonNull(sqsClient, "sqsClient must not be null!");
    }

    @Override
    public SendMessageResponse sendMessage(String queue, String message) {
        Objects.requireNonNull(queue, "queue must not be null!");
        Objects.requireNonNull(message, "message must not be null!");

        try {
            SendMessageRequest messageRequest = SendMessageRequest
                .builder()
                .queueUrl(queue)
                .messageBody(message)
                .build();

            return sqsClient.sendMessage(messageRequest);
        }
        catch (SqsException ex) {
            throw new SqsSendMessageException(
                "AwsSqsSendMessageExecutor failed. Error writing message body '"
                    + message
                    + "'!", ex);
        }
    }

    //    private SqsClient getSqsClient() {
    //        return SqsClient
    //            .builder()
    //            .region(Region.of(awsRegion))
    //            .credentialsProvider(credentialsProvider)
    //            //            .credentialsProvider(() -> new AwsCredentials() {
    //            //                @Override
    //            //                public String accessKeyId() {
    //            //                    return awsCredentialsOrchestrator.getAwsAccessKey();
    //            //                }
    //            //
    //            //                @Override
    //            //                public String secretAccessKey() {
    //            //                    return awsCredentialsOrchestrator.getAwsSecretKey();
    //            //                }
    //            //            })
    //            .build();
    //    }
}
