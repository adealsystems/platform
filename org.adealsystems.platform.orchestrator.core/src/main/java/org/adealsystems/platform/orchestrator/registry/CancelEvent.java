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

package org.adealsystems.platform.orchestrator.registry;

import java.util.Objects;

public final class CancelEvent implements EventDescriptor {
    private final String id;
    private String name;

    public static CancelEvent forId(String id){
        return new CancelEvent(id);
    }

    private CancelEvent(String id) {
        this.id = Objects.requireNonNull(id, "id must not be null!");
    }

    public CancelEvent withName(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null!");
        return this;
    }

    @Override
    public boolean isValid() {
        return name != null && !name.isEmpty();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isStartEvent() {
        return false;
    }

    @Override
    public boolean isStopEvent() {
        return true;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CancelEvent that = (CancelEvent) o;
        return Objects.equals(id, that.id) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "CancelEvent{" +
            "id='" + id + '\'' +
            ", name='" + name + '\'' +
            '}';
    }
}
