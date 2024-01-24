/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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
import java.util.Objects;

public abstract class JenkinsSpecialDateJobFactory {

    protected final String jenkinsUrl;
    protected final String jenkinsUsername;
    protected final String jenkinsToken;

    public JenkinsSpecialDateJobFactory(String url, String username, String token) {
        this.jenkinsUrl = Objects.requireNonNull(url, "url must not be null!");
        this.jenkinsUsername = Objects.requireNonNull(username, "username must not be null!");
        this.jenkinsToken = Objects.requireNonNull(token, "token must not be null!");
    }

    public abstract JenkinsSpecialDateJob getJob(LocalDate inputDate, DataIdentifier dataId);
}
