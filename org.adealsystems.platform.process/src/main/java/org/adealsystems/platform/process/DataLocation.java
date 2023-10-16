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

package org.adealsystems.platform.process;

import java.util.Objects;
import java.util.regex.Pattern;

public class DataLocation {
    static final String NAME_PATTERN_STRING = "[a-z][0-9a-z]*(-[0-9a-z]+)*";
    private static final Pattern NAME_PATTERN = Pattern.compile(NAME_PATTERN_STRING);

    static final String ID_PATTERN_STRING = "[a-z]{2,3}";
    private static final Pattern ID_PATTERN = Pattern.compile(ID_PATTERN_STRING);

    /**
     * Generic input location
     */
    public static final DataLocation INPUT = new DataLocation("input", "in");

    /**
     * Generic output location
     */
    public static final DataLocation OUTPUT = new DataLocation("output", "out");

    private final String name;
    private final String id;

    public DataLocation(String name) {
        Objects.requireNonNull(name, "name must not be null!");
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("name value '" + name + "' doesn't match the pattern '" + NAME_PATTERN_STRING + "'!");
        }
        this.name = name;
        this.id = name.substring(0, 3);
    }

    public DataLocation(String name, String id) {
        Objects.requireNonNull(name, "name must not be null!");
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("name value '" + name + "' doesn't match the pattern '" + NAME_PATTERN_STRING + "'!");
        }
        if (!ID_PATTERN.matcher(id).matches()) {
            throw new IllegalArgumentException("id value '" + id + "' doesn't match the pattern '" + ID_PATTERN_STRING + "'!");
        }
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataLocation that = (DataLocation) o;
        return Objects.equals(name, that.name) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id);
    }

    @Override
    public String toString() {
        return name;
    }
}
