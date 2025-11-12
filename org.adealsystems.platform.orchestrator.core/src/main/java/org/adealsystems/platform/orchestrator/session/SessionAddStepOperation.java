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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;

public class SessionAddStepOperation implements SessionUpdateOperation {
    private final LocalDateTime timestamp;
    private final String producer;

    private final EventProcessingStep step;

    public SessionAddStepOperation(EventProcessingStep step) {
        this(LocalDateTime.now(ZoneId.systemDefault()), Thread.currentThread().getName(), step);
    }

    @JsonCreator
    public SessionAddStepOperation(
        @JsonProperty("timestamp") LocalDateTime timestamp,
        @JsonProperty("producer") String producer,
        @JsonProperty("step") EventProcessingStep step
    ) {
        this.timestamp = timestamp;
        this.producer = producer;
        this.step = step;
    }

    @Override
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public String getProducer() {
        return producer;
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
        return Objects.equals(timestamp, that.timestamp)
            && Objects.equals(producer, that.producer)
            && Objects.equals(step, that.step);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, producer, step);
    }

    @Override
    public String toString() {
        return "SessionProcessingStateAddStepOperation{" +
            "timestamp=" + timestamp +
            ", producer='" + producer + '\'' +
            ", step=" + step +
            '}';
    }
}
