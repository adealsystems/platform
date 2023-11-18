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

package org.adealsystems.platform.orchestrator.executor.jenkins;

import java.util.Map;
import java.util.Objects;

public abstract class JenkinsJobBase {

    private final String jenkinsUrl;
    private final String jenkinsToken;
    private final String jenkinsUsername;

    private static final String DATA_PREFIX = " --data ";

    public JenkinsJobBase(String url, String username, String token) {
        this.jenkinsUrl = Objects.requireNonNull(url, "url must not be null!");
        this.jenkinsToken = Objects.requireNonNull(token, "token must not be null!");
        this.jenkinsUsername = Objects.requireNonNull(username, "username must not be null!");
    }

    protected abstract String getJenkinsJobName(Map<String, String> additionalParameters);

    protected String createCommand(String commandId, Map<String, String> additionalParameters) {
        StringBuilder parameters = new StringBuilder();
        if (additionalParameters != null && !additionalParameters.isEmpty()) {
            for (Map.Entry<String, String> entry : additionalParameters.entrySet()) {
                String name = entry.getKey();
                String value = entry.getValue();
                parameters.append(DATA_PREFIX).append(name).append('=').append(value);
            }
        }
        if (commandId != null) {
            parameters.append(DATA_PREFIX).append("commandId=").append(commandId);
        }

        String command = "curl -X POST"
            + " --user " + jenkinsUsername + ':' + jenkinsToken
            + ' ' + jenkinsUrl + "/job/" + getJenkinsJobName(additionalParameters);

        if (parameters.length() > 0) {
            command = command + "/buildWithParameters" + parameters;
        } else {
            command = command + "/build";
        }

        return command;
    }
}
