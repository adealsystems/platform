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

package org.adealsystems.platform.orchestrator.executor.email;

import org.adealsystems.platform.orchestrator.executor.CommandIdGenerator;

import java.util.Map;
import java.util.Objects;

public class EmailSenderFactory {
    private final String jenkinsUrl;
    private final String jenkinsToken;
    private final String jenkinsUserName;
    private final CommandIdGenerator commandIdGenerator;
    private final Map<RecipientsCluster, String> recipientClusterMapping;

    public EmailSenderFactory(
        String jenkinsUrl,
        String jenkinsUserName,
        String jenkinsToken,
        CommandIdGenerator commandIdGenerator,
        Map<RecipientsCluster, String> recipientClusterMapping
    ) {
        this.jenkinsUrl = jenkinsUrl;
        this.jenkinsToken = jenkinsToken;
        this.jenkinsUserName = jenkinsUserName;
        this.commandIdGenerator = Objects.requireNonNull(commandIdGenerator, "commandIdGenerator must not be null!");
        this.recipientClusterMapping = Objects.requireNonNull(recipientClusterMapping, "recipientClusterMapping must not be null!");
    }

    public EmailSender getSender(RecipientsCluster cluster, EmailType type) {
        String jobName = determineJobName(type);
        JenkinsEmailSenderJob job = new JenkinsEmailSenderJob(jenkinsUrl, jenkinsToken, jenkinsUserName, jobName);

        String recipients = recipientClusterMapping.get(cluster);
        return new JenkinsEmailSenderExecutor(recipients, job, commandIdGenerator);
    }

    private String determineJobName(EmailType type) {
        switch (type) {
            case INFO:
                return "INFO_EMAIL_SENDER";
            case ERROR:
                return "ERROR_EMAIL_SENDER";
            case EMAIL_WITH_ATTACHMENT:
                return "EMAIL_WITH_ATTACHMENT_SENDER";
            default:
                throw new IllegalArgumentException("Unknown/unsupported email type '" + type + "'!");
        }
    }
}
