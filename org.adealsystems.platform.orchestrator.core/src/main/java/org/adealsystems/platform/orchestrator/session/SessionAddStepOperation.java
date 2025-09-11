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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.adealsystems.platform.orchestrator.Session;
import org.adealsystems.platform.orchestrator.status.EventProcessingStep;

import java.util.Objects;

public class SessionAddStepOperation implements SessionUpdateOperation {
    private final EventProcessingStep step;

    @JsonCreator
    public SessionAddStepOperation(
        @JsonProperty("step") EventProcessingStep step
    ) {
        this.step = step;
    }

    @Override
    public void apply(Session session) {
        session.getProcessingState().addStep(step);
    }

    public EventProcessingStep getStep() {
        return step;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SessionAddStepOperation that = (SessionAddStepOperation) o;
        return Objects.equals(step, that.step);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(step);
    }

    @Override
    public String toString() {
        return "SessionProcessingStateAddStepOperation{" +
            "step=" + step +
            '}';
    }
}
