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
import org.adealsystems.platform.orchestrator.status.SessionProcessingState;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;

public class SessionUpdateProgressOperation implements SessionUpdateOperation {
    private final LocalDateTime timestamp;
    private final String producer;

    public SessionUpdateProgressOperation() {
        this(LocalDateTime.now(ZoneId.systemDefault()), Thread.currentThread().getName());
    }

    @JsonCreator
    public SessionUpdateProgressOperation(
        @JsonProperty("timestamp") LocalDateTime timestamp,
        @JsonProperty("producer") String producer
    ) {
        this.timestamp = timestamp;
        this.producer = producer;
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
        SessionProcessingState processingState = session.getProcessingState();
        processingState.setProgressCurrentStep(processingState.getProgressCurrentStep() + 1);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SessionUpdateProgressOperation that = (SessionUpdateProgressOperation) o;
        return Objects.equals(timestamp, that.timestamp) && Objects.equals(producer, that.producer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, producer);
    }

    @Override
    public String toString() {
        return "SessionUpdateProgressOperation{" +
            "timestamp=" + timestamp +
            ", producer='" + producer + '\'' +
            '}';
    }
}
