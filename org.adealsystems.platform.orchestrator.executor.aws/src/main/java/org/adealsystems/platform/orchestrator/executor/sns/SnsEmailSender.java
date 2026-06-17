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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.adealsystems.platform.orchestrator.AwsCredentialsOrchestrator;
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode;
import org.adealsystems.platform.orchestrator.executor.ExecutorResult;
import org.adealsystems.platform.orchestrator.executor.email.EmailSender;
import org.adealsystems.platform.orchestrator.executor.email.EmailType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.adealsystems.platform.orchestrator.executor.ExecutorExitCode.ERROR;
import static org.adealsystems.platform.orchestrator.executor.ExecutorExitCode.SUCCESS;

public class SnsEmailSender implements EmailSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnsEmailSender.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String LINK_TOKEN = "<link>";

    private final String environment;

    private final String topicArn;

    private final EmailType type;

    private final AwsCredentialsOrchestrator awsCredentialsOrchestrator;

    public SnsEmailSender(String environment, String topicArn, EmailType type, AwsCredentialsOrchestrator awsCredentialsOrchestrator) {
        this.environment = Objects.requireNonNull(environment, "environment must not be null!");
        this.topicArn = Objects.requireNonNull(topicArn, "topicArn must not be null!");
        this.type = Objects.requireNonNull(type, "type must not be null!");
        this.awsCredentialsOrchestrator = Objects.requireNonNull(awsCredentialsOrchestrator, "awsCredentialsOrchestrator must not be null!");
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message) {
        return publish(subject, message, null, null, null, null, null);
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendEmailWithLink(String subject, String message, String linkName) {
        return sendEmailWithLink(subject, message, "U:\\islisten-Kapazitaetsmanagement\\ARMS\\" + linkName, linkName);
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendEmailWithLink(String subject, String message, String linkTarget, String linkName) {
        String linkedMessage = message;
        if (linkedMessage != null && linkedMessage.contains(LINK_TOKEN)) {
            linkedMessage = linkedMessage.replace(LINK_TOKEN, "<a href=\"" + linkTarget + "\">" + linkName + "</a>");
        }
        return publish(subject, linkedMessage, null, null, null, linkTarget, linkName);
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message, String parameter) {
        return publish(subject, message, parameter, null, null, null, null);
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message, String parameter, String filename, String attachmentName) {
        return publish(subject, message, parameter, filename, attachmentName, null, null);
    }

    private ExecutorResult<ExecutorExitCode> publish(
        String subject,
        String message,
        String parameter,
        String filename,
        String attachmentName,
        String linkTarget,
        String linkName
    ) {
        Objects.requireNonNull(subject, "subject must not be null!");

        LOGGER.info("About to publish email request to SNS topic '{}' for subject '{}'", topicArn, subject);

        String payload;
        try {
            payload = createPayload(subject, message, parameter, filename, attachmentName, linkTarget, linkName);
        } catch (JsonProcessingException ex) {
            LOGGER.error("Unable to serialize SNS email request for subject '{}'!", subject, ex);
            return new ExecutorResult<>(ERROR, topicArn, "Unable to serialize SNS email request!", ex);
        }

        PublishRequest request = PublishRequest.builder()
            .topicArn(topicArn)
            .message(payload)
            .build();

        try (SnsClient snsClient = getSnsClient()) {
            PublishResponse response = snsClient.publish(request);
            return new ExecutorResult<>(SUCCESS, topicArn, "Message ID: " + response.messageId());
        } catch (SnsException ex) {
            LOGGER.error("SnsEmailSender failed for topic '{}'!", topicArn, ex);
            return new ExecutorResult<>(ERROR, topicArn, getErrorMessage(ex), ex);
        } catch (RuntimeException ex) {
            LOGGER.error("SnsEmailSender failed for topic '{}'!", topicArn, ex);
            return new ExecutorResult<>(ERROR, topicArn, "Unable to publish SNS email request!", ex);
        }
    }

    private String createPayload(
        String subject,
        String message,
        String parameter,
        String filename,
        String attachmentName,
        String linkTarget,
        String linkName
    ) throws JsonProcessingException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("environment", environment);
        payload.put("type", type.name());
        payload.put("subject", subject);
        payload.put("message", message);
        putIfNotNull(payload, "parameter", parameter);
        putIfNotNull(payload, "filename", filename);
        putIfNotNull(payload, "attachmentName", attachmentName);
        putIfNotNull(payload, "linkTarget", linkTarget);
        putIfNotNull(payload, "linkName", linkName);
        return OBJECT_MAPPER.writeValueAsString(payload);
    }

    private void putIfNotNull(Map<String, Object> payload, String key, Object value) {
        if (value != null) {
            payload.put(key, value);
        }
    }

    private String getErrorMessage(SnsException ex) {
        if (ex.awsErrorDetails() == null) {
            return ex.getMessage();
        }
        return ex.awsErrorDetails().errorMessage();
    }

    public SnsClient getSnsClient() {
        return SnsClient.builder()
            .region(Region.of(awsCredentialsOrchestrator.getAwsRegion()))
            .credentialsProvider(() -> new AwsCredentials() {
                @Override
                public String accessKeyId() {
                    return awsCredentialsOrchestrator.getAwsAccessKey();
                }

                @Override
                public String secretAccessKey() {
                    return awsCredentialsOrchestrator.getAwsSecretKey();
                }
            })
            .build();
    }
}
