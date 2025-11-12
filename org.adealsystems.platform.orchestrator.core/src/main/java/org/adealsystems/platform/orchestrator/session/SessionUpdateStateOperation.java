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
import org.adealsystems.platform.orchestrator.status.State;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;

public class SessionUpdateStateOperation implements SessionUpdateOperation {
    private final LocalDateTime timestamp;
    private final String producer;
    private final State state;

    public SessionUpdateStateOperation(State state) {
        this(LocalDateTime.now(ZoneId.systemDefault()), Thread.currentThread().getName(), state);
    }

    @JsonCreator
    public SessionUpdateStateOperation(
        @JsonProperty("timestamp") LocalDateTime timestamp,
        @JsonProperty("producer") String producer,
        @JsonProperty("state") State state
    ) {
        this.timestamp = timestamp;
        this.producer = producer;
        this.state = state;
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
        Objects.requireNonNull(session, "session must not be null!");

        session.getProcessingState().setState(state);
    }

    public State getState() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SessionUpdateStateOperation that = (SessionUpdateStateOperation) o;
        return Objects.equals(timestamp, that.timestamp)
            && Objects.equals(producer, that.producer)
            && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, producer, state);
    }

    @Override
    public String toString() {
        return "SessionUpdateStateOperation{" +
            "timestamp=" + timestamp +
            ", producer='" + producer + '\'' +
            ", state=" + state +
            '}';
    }
}
