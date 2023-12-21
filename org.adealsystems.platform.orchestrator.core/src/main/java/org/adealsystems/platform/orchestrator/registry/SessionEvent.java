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

package org.adealsystems.platform.orchestrator.registry;

import java.util.Objects;

public final class SessionEvent implements EventDescriptor {

    public enum LifecycleType {
        ANY,
        START,
        STOP,
        RESUME,
        CHANGE
    }

    private final String id;
    private final String instanceRef;
    private boolean startEvent;
    private boolean stopEvent;
    private LifecycleType type = LifecycleType.STOP;

    public static SessionEvent forIdAndInstance(String id, String instanceRef){
        return new SessionEvent(id, instanceRef);
    }

    private SessionEvent(String id, String instanceRef) {
        this.id = Objects.requireNonNull(id, "id must not be null!");
        this.instanceRef = Objects.requireNonNull(instanceRef, "instanceRef must not be null!");
    }

    public SessionEvent asStartEvent() {
        this.startEvent = true;
        return this;
    }

    public SessionEvent asStopEvent() {
        this.stopEvent = true;
        return this;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    public SessionEvent forType(LifecycleType type) {
        this.type = type;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isStartEvent() {
        return startEvent;
    }

    @Override
    public boolean isStopEvent() {
        return stopEvent;
    }

    public LifecycleType getType() {
        return type;
    }

    public String getInstanceRef() {
        return instanceRef;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionEvent that = (SessionEvent) o;
        return startEvent == that.startEvent
            && stopEvent == that.stopEvent
            && Objects.equals(id, that.id)
            && Objects.equals(instanceRef, that.instanceRef)
            && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, instanceRef, startEvent, stopEvent, type);
    }

    @Override
    public String toString() {
        return "SessionEvent{" +
            "id='" + id + '\'' +
            ", instanceRef='" + instanceRef + '\'' +
            ", startEvent=" + startEvent +
            ", stopEvent=" + stopEvent +
            ", type=" + type +
            '}';
    }
}
