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

package org.adealsystems.platform.orchestrator.executor.jenkins;

import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.orchestrator.executor.CommandIdGenerator;
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode;
import org.adealsystems.platform.orchestrator.executor.ExecutorResult;
import org.adealsystems.platform.orchestrator.executor.SingleJobExecutor;

import java.util.Objects;

import static org.adealsystems.platform.orchestrator.executor.ProcessExecutor.processCommand;

public class JenkinsJobExecutor implements SingleJobExecutor {

    private final CommandIdGenerator commandIdGenerator;

    private final JenkinsJobFactory jobFactory;

    public JenkinsJobExecutor(JenkinsJobFactory jobFactory, CommandIdGenerator commandIdGenerator) {
        this.jobFactory = Objects.requireNonNull(jobFactory, "jobFactory must not be null!");
        this.commandIdGenerator = Objects.requireNonNull(commandIdGenerator, "commandIdGenerator must not be null!");
    }

    @Override
    public ExecutorResult<ExecutorExitCode> execute(DataIdentifier dataId) {
        String commandId = commandIdGenerator.generate();

        JenkinsJob job = jobFactory.getJob(dataId);
        if (job == null) {
            return new ExecutorResult<>(ExecutorExitCode.UNDEFINED, commandId, "No jenkins job is configured for " + dataId + "!");
        }

        String command = job.createCommand(commandId, dataId);
        return processCommand(command, commandId);
    }
}
