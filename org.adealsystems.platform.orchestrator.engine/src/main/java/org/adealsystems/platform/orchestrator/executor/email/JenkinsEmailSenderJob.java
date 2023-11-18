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

import org.adealsystems.platform.orchestrator.executor.jenkins.JenkinsJobBase;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class JenkinsEmailSenderJob extends JenkinsJobBase {
    public static final String RECIPIENTS = "recipients";
    public static final String SUBJECT = "subject";
    public static final String MESSAGE = "message";
    public static final String PARAMETER = "argument";
    public static final String FILENAME = "filename";
    public static final String ATTACHMENT_NAME = "attachment_name";

    private final String jenkinsJobName;

    public JenkinsEmailSenderJob(String url, String username, String token, String jobName) {
        super(url, username, token);

        this.jenkinsJobName = Objects.requireNonNull(jobName, "jobName must not be null!");
    }

    @Override
    protected String getJenkinsJobName(Map<String, String> additionalParameters) {
        return jenkinsJobName;
    }

    private Map<String, String> prepareAdditionalParameters(String recipients, String subject, String message, String parameter) {
        Objects.requireNonNull(recipients, "recipients must not be null!");
        Objects.requireNonNull(subject, "subject must not be null!");
        Objects.requireNonNull(message, "message must not be null!");

        recipients = recipients.toLowerCase(Locale.ROOT).trim().replaceAll(" ", "");

        Map<String, String> additionalParameter = new HashMap<>();
        additionalParameter.put(RECIPIENTS, recipients);
        try {
            additionalParameter.put(SUBJECT, URLEncoder.encode(subject, StandardCharsets.UTF_8.name()));
            additionalParameter.put(MESSAGE, URLEncoder.encode(message, StandardCharsets.UTF_8.name()));
            if (parameter != null) {
                additionalParameter.put(PARAMETER, URLEncoder.encode(parameter, StandardCharsets.UTF_8.name()));
            }
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException("Unable to url-encode value!", ex);
        }

        return additionalParameter;
    }

    private Map<String, String> prepareAdditionalParameters(String recipients, String subject, String message) {
        Objects.requireNonNull(recipients, "recipients must not be null!");
        Objects.requireNonNull(subject, "subject must not be null!");

        recipients = recipients.toLowerCase(Locale.ROOT).trim().replaceAll(" ", "");

        Map<String, String> additionalParameter = new HashMap<>();
        additionalParameter.put(RECIPIENTS, recipients);
        try {
            additionalParameter.put(SUBJECT, URLEncoder.encode(subject, StandardCharsets.UTF_8.name()));
            if (message != null) {
                additionalParameter.put(MESSAGE, URLEncoder.encode(message, StandardCharsets.UTF_8.name()));
            }
            else {
                additionalParameter.put(MESSAGE, URLEncoder.encode("No message available!", StandardCharsets.UTF_8.name()));
            }
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException("Unable to url-encode value!", ex);
        }

        return additionalParameter;
    }

    private Map<String, String> prepareAdditionalParameters(String recipients, String subject, String message, String parameter, String filename, String attachmentName) {
        Objects.requireNonNull(recipients, "recipients must not be null!");
        Objects.requireNonNull(subject, "subject must not be null!");
        Objects.requireNonNull(message, "message must not be null!");
        Objects.requireNonNull(filename, "filename must not be null!");

        recipients = recipients.toLowerCase(Locale.ROOT).trim().replaceAll(" ", "");

        Map<String, String> additionalParameter = new HashMap<>();
        additionalParameter.put(RECIPIENTS, recipients);
        try {
            additionalParameter.put(SUBJECT, URLEncoder.encode(subject, StandardCharsets.UTF_8.name()));
            additionalParameter.put(MESSAGE, URLEncoder.encode(message, StandardCharsets.UTF_8.name()));
            if (parameter != null) {
                additionalParameter.put(PARAMETER, URLEncoder.encode(parameter, StandardCharsets.UTF_8.name()));
            }
            additionalParameter.put(FILENAME, URLEncoder.encode(filename, StandardCharsets.UTF_8.name()));
            if (attachmentName != null) {
                additionalParameter.put(ATTACHMENT_NAME, URLEncoder.encode(attachmentName, StandardCharsets.UTF_8.name()));
            }
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException("Unable to url-encode value!", ex);
        }

        return additionalParameter;
    }

    public String createCommand(String recipients, String subject, String message, String parameter) {
        Objects.requireNonNull(recipients, "Recipient must not be null!");

        Map<String, String> additionalParameters = prepareAdditionalParameters(recipients, subject, message, parameter);
        return createCommand(null, additionalParameters);
    }

    public String createCommand(String recipients, String subject, String message) {
        Objects.requireNonNull(recipients, "Recipient must not be null!");

        Map<String, String> additionalParameters = prepareAdditionalParameters(recipients, subject, message);
        return createCommand(null, additionalParameters);
    }

    public String createCommand(String recipients, String subject, String message, String parameter, String filename, String attachmentName) {
        Objects.requireNonNull(recipients, "Recipient must not be null!");

        Map<String, String> additionalParameters = prepareAdditionalParameters(recipients, subject, message, parameter, filename, attachmentName);
        return createCommand(null, additionalParameters);
    }
}
