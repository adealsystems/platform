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

import java.time.LocalDate;
import java.util.Map;
import java.util.Objects;

public abstract class JenkinsSpecialDateJob extends JenkinsJobBase {

    public JenkinsSpecialDateJob(String url, String username, String token) {
        super(url, username, token);
    }

    protected abstract Map<String, String> prepareAdditionalParameters(LocalDate inputDate, DataIdentifier dataIdentifier);

    public String createCommand(String commandId, LocalDate inputDate, DataIdentifier dataIdentifier) {
        Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");
        Map<String, String> additionalParameters = prepareAdditionalParameters(inputDate, dataIdentifier);
        return createCommand(commandId, additionalParameters);
    }
}
