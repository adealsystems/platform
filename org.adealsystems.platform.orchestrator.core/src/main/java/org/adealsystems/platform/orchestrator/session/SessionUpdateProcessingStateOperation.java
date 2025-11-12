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

public class SessionUpdateProcessingStateOperation implements SessionUpdateOperation {
    private final LocalDateTime timestamp;
    private final String producer;
    private final String cause;

    private final SessionProcessingState processingState;

    public SessionUpdateProcessingStateOperation(SessionProcessingState processingState) {
        this(
            LocalDateTime.now(ZoneId.systemDefault()),
            Thread.currentThread().getName(),
            buildCause(),
            processingState
        );
    }

    private static String buildCause() {
        Throwable cause = new Throwable();
        StringBuilder builder = new StringBuilder();
        StackTraceElement[] stackTrace = cause.getStackTrace();
        if (stackTrace != null) {
            for (StackTraceElement element : stackTrace) {
                if (builder.length() > 0) {
                    builder.append(" -> ");
                }
                builder.append(element.toString());
            }
        }
        return builder.toString();
    }

    @JsonCreator
    public SessionUpdateProcessingStateOperation(
        @JsonProperty("timestamp") LocalDateTime timestamp,
        @JsonProperty("producer") String producer,
        @JsonProperty("cause") String cause,
        @JsonProperty("processingState") SessionProcessingState processingState
    ) {
        this.timestamp = timestamp;
        this.producer = producer;
        this.cause = cause;
        this.processingState = processingState == null ? null : SessionProcessingState.clone(processingState);
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

        session.setProcessingState(processingState);
    }

    public String getCause() {
        return cause;
    }

    public SessionProcessingState getProcessingState() {
        return processingState;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SessionUpdateProcessingStateOperation that = (SessionUpdateProcessingStateOperation) o;
        return Objects.equals(timestamp, that.timestamp)
            && Objects.equals(producer, that.producer)
            && Objects.equals(processingState, that.processingState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, producer, processingState);
    }

    @Override
    public String toString() {
        return "SessionUpdateProcessingStateOperation{" +
            "timestamp=" + timestamp +
            ", producer='" + producer + '\'' +
            ", processingState=" + processingState +
            '}';
    }
}
