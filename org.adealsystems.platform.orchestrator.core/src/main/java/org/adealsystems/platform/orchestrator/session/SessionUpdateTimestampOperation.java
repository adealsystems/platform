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

public class SessionUpdateTimestampOperation implements SessionUpdateOperation {
    private final LocalDateTime timestamp;

    private final SessionTimestamp timestampType;
    private final LocalDateTime value;

    public SessionUpdateTimestampOperation(SessionTimestamp timestampType, LocalDateTime value) {
        this(LocalDateTime.now(ZoneId.systemDefault()), timestampType, value);
    }

    @JsonCreator
    public SessionUpdateTimestampOperation(
        @JsonProperty("timestamp") LocalDateTime timestamp,
        @JsonProperty("ts-type") SessionTimestamp timestampType,
        @JsonProperty("value") LocalDateTime value
    ) {
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.value = value;
    }

    @Override
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public void apply(Session session) {
        switch (timestampType) {
            case STARTED:
                session.getProcessingState().setStarted(value);
                break;
            case UPDATED:
                session.getProcessingState().setLastUpdated(value);
                break;
            case TERMINATED:
                session.getProcessingState().setTerminated(value);
                break;
        }
    }

    public SessionTimestamp getTimestampType() {
        return timestampType;
    }

    public LocalDateTime getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SessionUpdateTimestampOperation that = (SessionUpdateTimestampOperation) o;
        return Objects.equals(timestamp, that.timestamp)
            && timestampType == that.timestampType
            && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, timestampType, value);
    }

    @Override
    public String toString() {
        return "SessionUpdateTimestampOperation{" +
            "timestamp=" + timestamp +
            ", timestampType=" + timestampType +
            ", value=" + value +
            '}';
    }
}
