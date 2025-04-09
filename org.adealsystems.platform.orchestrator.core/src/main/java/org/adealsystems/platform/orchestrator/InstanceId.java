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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.regex.Pattern;

public final class InstanceId implements Comparable<InstanceId> {
    public static final String PATTERN_STRING = "[0-9]{4}([-_][0-9a-z/]+)*";
    private static final Pattern PATTERN = Pattern.compile(PATTERN_STRING);

    private final String id;

    @JsonCreator
    public InstanceId(String id) {
        if (id == null) {
            throw new InstanceIdCreationException("id must not be null!");
        }
        if (!PATTERN.matcher(id).matches()) {
            throw new InstanceIdCreationException("id value doesn't match the pattern '" + PATTERN.pattern() + "'!", id);
        }

        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public int compareTo(InstanceId other) {
        if (other == null) {
            return 1;
        }

        return this.id.compareTo(other.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstanceId)) return false;
        InstanceId that = (InstanceId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    @JsonValue
    public String toString() {
        return id;
    }
}
