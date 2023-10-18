/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

import java.util.Objects;

public class EventAffiliation {
    public static final EventAffiliation RAW = new EventAffiliation(null, null);

    private final InstanceId instanceId;
    private final SessionId sessionId;
    private final boolean processed;

    public EventAffiliation(InstanceId instanceId, SessionId sessionId) {
        this.instanceId = instanceId;
        this.sessionId = sessionId;
        this.processed = false;
    }

    public EventAffiliation(InstanceId instanceId, SessionId sessionId, boolean processed) {
        this.instanceId = instanceId;
        this.sessionId = sessionId;
        this.processed = processed;
    }

    public InstanceId getInstanceId() {
        return instanceId;
    }

    public SessionId getSessionId() {
        return sessionId;
    }

    public boolean isProcessed() {
        return processed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventAffiliation)) return false;
        EventAffiliation that = (EventAffiliation) o;
        return processed == that.processed && Objects.equals(instanceId, that.instanceId) && Objects.equals(sessionId, that.sessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, sessionId, processed);
    }

    @Override
    public String toString() {
        return "EventAffiliation{" +
            "instanceId=" + instanceId +
            ", sessionId=" + sessionId +
            ", processed=" + processed +
            '}';
    }
}
