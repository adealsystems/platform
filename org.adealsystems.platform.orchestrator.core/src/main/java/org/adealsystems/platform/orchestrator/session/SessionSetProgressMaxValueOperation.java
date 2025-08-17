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

package org.adealsystems.platform.orchestrator.session;

import org.adealsystems.platform.orchestrator.Session;

import java.util.Objects;

public class SessionSetProgressMaxValueOperation implements SessionUpdateOperation {
    private final int progressMaxValue;

    public SessionSetProgressMaxValueOperation(int progressMaxValue) {
        this.progressMaxValue = progressMaxValue;
    }

    @Override
    public void apply(Session session) {
        session.getProcessingState().setProgressMaxValue(progressMaxValue);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SessionSetProgressMaxValueOperation that = (SessionSetProgressMaxValueOperation) o;
        return progressMaxValue == that.progressMaxValue;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(progressMaxValue);
    }

    @Override
    public String toString() {
        return "SessionProcessingStateSetProgressMaxValueOperation{" +
            "progressMaxValue=" + progressMaxValue +
            '}';
    }
}
