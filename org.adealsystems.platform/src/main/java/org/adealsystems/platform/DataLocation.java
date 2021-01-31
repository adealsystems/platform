/*
 * Copyright 2020-2021 ADEAL Systems GmbH
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

package org.adealsystems.platform;

import java.util.Objects;
import java.util.regex.Pattern;

public class DataLocation {
    static final String PATTERN_STRING = "[a-z][0-9a-z]*(-[0-9a-z]+)*";
    private static final Pattern PATTERN = Pattern.compile(PATTERN_STRING);

    /**
     * Generic input location
     */
    public static final DataLocation INPUT = new DataLocation("input");

    /**
     * Generic output location
     */
    public static final DataLocation OUTPUT = new DataLocation("output");


    private final String name;

    public DataLocation(String name) {
        Objects.requireNonNull(name, "name must not be null!");
        if (!PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("name value '" + name + "' doesn't match the pattern '" + PATTERN_STRING + "'!");
        }
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataLocation that = (DataLocation) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
