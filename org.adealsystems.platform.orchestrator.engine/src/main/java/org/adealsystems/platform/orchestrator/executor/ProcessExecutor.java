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

package org.adealsystems.platform.orchestrator.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ProcessExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessExecutor.class);

    private static final Pattern RESPONSE_TITLE_PATTERN = Pattern.compile(".*<title>(.*)</title>.*");
    private static final String RESPONSE_TITLE_404 = "Error 404 Not Found";

    private ProcessExecutor() {
    }

    public static ExecutorResult<ExecutorExitCode> processCommand(String command, String commandId) {
        return processCommand(command, commandId, true);
    }

    public static ExecutorResult<ExecutorExitCode> processCommand(String command, String commandId, boolean htmlResponse) {
        LOGGER.info("About to process command '{}' with id '{}'", command, commandId);

        ProcessBuilder processBuilder = new ProcessBuilder(command.split(" "));
        processBuilder.redirectErrorStream(true);

        Process process = null;
        int exitCode;
        try {
            process = processBuilder.start();
            exitCode = process.waitFor();
            if (exitCode == 0) {
                if (htmlResponse) {
                    try (
                        InputStream in = process.getInputStream();
                        BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                    ) {
                        while (br.ready()) {
                            String line = br.readLine().trim();
                            Matcher matcher = RESPONSE_TITLE_PATTERN.matcher(line);
                            if (matcher.matches()) {
                                String title = matcher.group(1);
                                if (RESPONSE_TITLE_404.equals(title)) {
                                    return new ExecutorResult<>(ExecutorExitCode.ERROR, commandId, "Requested URL resource does not found!");
                                }
                                break;
                            }
                        }
                    } catch (Exception ex) {
                        return new ExecutorResult<>(ExecutorExitCode.ERROR, commandId, "Unable to read command's output!");
                    }
                }
                return new ExecutorResult<>(ExecutorExitCode.SUCCESS, commandId);
            }
            return new ExecutorResult<>(ExecutorExitCode.ERROR, commandId, "Exit value of processCommand is unequal 0!");
        } catch (IOException | InterruptedException ex) {
            LOGGER.error("Error processing step!", ex);
            return new ExecutorResult<>(ExecutorExitCode.ERROR, commandId, "Error processing step! ", ex);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }
}
