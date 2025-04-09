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

import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode;
import org.adealsystems.platform.orchestrator.executor.ExecutorResult;


public interface EmailSender {
    ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message);
    ExecutorResult<ExecutorExitCode> sendEmailWithLink(String subject, String message, String linkName);
    ExecutorResult<ExecutorExitCode> sendEmailWithLink(String subject, String message, String linkTarget, String linkName);
    ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message, String parameter);
    ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message, String parameter, String filename, String attachmentName);
}
