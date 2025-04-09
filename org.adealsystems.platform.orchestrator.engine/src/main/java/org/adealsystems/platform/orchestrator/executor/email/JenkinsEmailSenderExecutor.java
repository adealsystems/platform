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

package org.adealsystems.platform.orchestrator.executor.email;

import org.adealsystems.platform.orchestrator.executor.CommandIdGenerator;
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode;
import org.adealsystems.platform.orchestrator.executor.ExecutorResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.adealsystems.platform.orchestrator.executor.ProcessExecutor.processCommand;

public class JenkinsEmailSenderExecutor implements EmailSender {
    public static final String LINK_TOKEN = "<link>";
    private static final Logger LOGGER = LoggerFactory.getLogger(JenkinsEmailSenderExecutor.class);

    private final String recipients;

    private final JenkinsEmailSenderJob senderJob;

    private final CommandIdGenerator commandIdGenerator;

    public JenkinsEmailSenderExecutor(String recipients, JenkinsEmailSenderJob senderJob, CommandIdGenerator commandIdGenerator) {
        this.recipients = Objects.requireNonNull(recipients, "recipients must not be null!");
        this.senderJob = Objects.requireNonNull(senderJob, "senderJob must not be null!");
        this.commandIdGenerator = Objects.requireNonNull(commandIdGenerator, "commandIdGenerator must not be null!");
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message, String parameter) {
        Objects.requireNonNull(subject, "subject must not be null!");

        LOGGER.info("About to execute send-email-job for subject '{}', message '{}' and parameter '{}'", subject, message, parameter);

        String commandId = commandIdGenerator.generate();

        String command = senderJob.createCommand(recipients, subject, message, parameter);
        return processCommand(command, commandId);
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message) {
        Objects.requireNonNull(subject, "subject must not be null!");

        LOGGER.info("About to execute send-email-job for subject '{}' and message '{}'", subject, message);

        String commandId = commandIdGenerator.generate();

        String command = senderJob.createCommand(recipients, subject, message);
        return processCommand(command, commandId);
    }
    @Override
    public ExecutorResult<ExecutorExitCode> sendEmailWithLink(String subject, String message, String linkTarget, String linkName) {
        Objects.requireNonNull(subject, "subject must not be null!");

        LOGGER.info("About to execute send-email-job for subject '{}' and message '{}'", subject, message);

        if(message.contains(LINK_TOKEN)){
            message = message.replace(LINK_TOKEN, "<a href=\"" + linkTarget + "\">" + linkName + "</a>");
        }

        String commandId = commandIdGenerator.generate();

        String command = senderJob.createCommand(recipients, subject, message);
        return processCommand(command, commandId);
    }

    /**
     * Returns sendEmailWithLink(String subject, String message, String linkTarget, String linkName)
     * <p>
     * with linkTarget: U:\\islisten-Kapazitaetsmanagement\\ARMS\\'linkName'
     */
    @Override
    public ExecutorResult<ExecutorExitCode> sendEmailWithLink(String subject, String message, String linkName) {
        return sendEmailWithLink(subject, message, "U:\\islisten-Kapazitaetsmanagement\\ARMS\\" + linkName, linkName);
    }

    @Override
    public ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message, String parameter, String filename, String attachmentName) {
        Objects.requireNonNull(subject, "subject must not be null!");

        LOGGER.info("About to execute send-email-job for subject '{}' and message '{}'", subject, message);

        String commandId = commandIdGenerator.generate();

        String command = senderJob.createCommand(recipients, subject, message, parameter, filename, attachmentName);
        return processCommand(command, commandId);
    }
}
