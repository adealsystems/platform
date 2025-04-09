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

package org.adealsystems.platform.orchestrator;

import java.beans.ConstructorProperties;
import java.time.LocalDateTime;
import java.util.Objects;

public class RunSpecification {
    private final RunType type;
    private final String id;

    public static boolean isEventOutdated(InternalEvent event, RunType type, RunRepository runRepository) {
        LocalDateTime runStartTimestamp;
        switch (type) {
            case ACTIVE:
                runStartTimestamp = runRepository.retrieveActiveRunStartTimestamp().orElse(null);
                break;
            case WAITING:
                runStartTimestamp = runRepository.retrieveWaitingRunStartTimestamp().orElse(null);
                break;
            default:
                throw new IllegalArgumentException("Unknown/unsupported run type '" + type + "'!");
        }

        if (runStartTimestamp == null) {
            return true;
        }

        LocalDateTime eventTimestamp = event.getTimestamp();
        if (eventTimestamp == null) {
            throw new IllegalStateException("Missing mandatory event field timestamp event: " + event + "!");
        }
        return eventTimestamp.isBefore(runStartTimestamp);
    }

    @ConstructorProperties({"type", "id"})
    public RunSpecification(RunType type, String id) {
        this.type = type;
        this.id = id;
    }

    public RunType getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RunSpecification)) return false;
        RunSpecification runId = (RunSpecification) o;
        return type == runId.type && Objects.equals(id, runId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, id);
    }

    @Override
    public String toString() {
        return "Run{" +
            "type=" + type +
            ", id='" + id + '\'' +
            '}';
    }
}
