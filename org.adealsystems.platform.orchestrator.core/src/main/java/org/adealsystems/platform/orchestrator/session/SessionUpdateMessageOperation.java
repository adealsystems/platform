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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;

public class SessionUpdateMessageOperation implements SessionUpdateOperation {
    private final LocalDateTime timestamp;

    private final String message;

    public SessionUpdateMessageOperation(String message) {
        this(LocalDateTime.now(ZoneId.systemDefault()), message);
    }

    @JsonCreator
    public SessionUpdateMessageOperation(
        @JsonProperty("timestamp") LocalDateTime timestamp,
        @JsonProperty("message") String message
    ) {
        this.timestamp = timestamp;
        this.message = message;
    }

    @Override
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public void apply(Session session) {
        session.getProcessingState().setMessage(message);
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SessionUpdateMessageOperation that = (SessionUpdateMessageOperation) o;
        return Objects.equals(timestamp, that.timestamp) && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, message);
    }

    @Override
    public String toString() {
        return "SessionProcessingStateUpdateMessageOperation{" +
            "timestamp=" + timestamp +
            ", message='" + message + '\'' +
            '}';
    }
}
