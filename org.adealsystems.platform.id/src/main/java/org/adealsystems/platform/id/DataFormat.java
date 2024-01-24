/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

package org.adealsystems.platform.id;

import java.util.Objects;

public enum DataFormat {
    CSV_COMMA(".csv"),
    CSV_SEMICOLON(".csv"),
    CSV_PIPE(".csv"),
    JSON(".json"),
    AVRO(".avro"),
    PARQUET(".parquet"),
    STATE(".state"),
    ATHENA(".<athena>"),
    JDBC(".<jdbc>"),
    EMAIL(".<email>");


    private final String extension;

    DataFormat(String extension) {
        Objects.requireNonNull(extension);
        if (extension.charAt(0) != '.') {
            throw new IllegalArgumentException("Extension must start with a '.'!");
        }
        this.extension = extension;
    }

    public String getExtension() {
        return extension;
    }
}
