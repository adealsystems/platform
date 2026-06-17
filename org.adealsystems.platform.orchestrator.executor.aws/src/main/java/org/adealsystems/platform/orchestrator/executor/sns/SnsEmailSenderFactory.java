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

package org.adealsystems.platform.orchestrator.executor.sns;

import org.adealsystems.platform.orchestrator.AwsCredentialsOrchestrator;
import org.adealsystems.platform.orchestrator.executor.email.EmailSender;
import org.adealsystems.platform.orchestrator.executor.email.EmailSenderFactory;
import org.adealsystems.platform.orchestrator.executor.email.EmailType;
import org.adealsystems.platform.orchestrator.executor.email.RecipientsCluster;

import java.util.Map;
import java.util.Objects;

public class SnsEmailSenderFactory implements EmailSenderFactory {
    private final AwsCredentialsOrchestrator awsCredentialsOrchestrator;

    private final Map<RecipientsCluster, String> topicArnMapping;

    public SnsEmailSenderFactory(AwsCredentialsOrchestrator awsCredentialsOrchestrator, Map<RecipientsCluster, String> topicArnMapping) {
        this.awsCredentialsOrchestrator = Objects.requireNonNull(awsCredentialsOrchestrator, "awsCredentialsOrchestrator must not be null!");
        this.topicArnMapping = Objects.requireNonNull(topicArnMapping, "topicArnMapping must not be null!");
    }

    @Override
    public EmailSender getSender(String env, RecipientsCluster cluster, EmailType type) {
        String topicArn = determineTopicArn(cluster);
        return new SnsEmailSender(env, topicArn, type, awsCredentialsOrchestrator);
    }

    @Override
    public EmailSender getSender(String env, String recipients, EmailType type) {
        return new SnsEmailSender(env, recipients, type, awsCredentialsOrchestrator);
    }

    private String determineTopicArn(RecipientsCluster cluster) {
        String result = topicArnMapping.get(cluster);
        if (result == null) {
            throw new IllegalArgumentException("Unknown/unsupported recipients cluster '" + cluster + "'!");
        }
        return result;
    }
}
