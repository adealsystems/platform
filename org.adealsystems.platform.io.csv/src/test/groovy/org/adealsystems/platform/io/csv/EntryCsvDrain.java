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

package org.adealsystems.platform.io.csv;

import org.adealsystems.platform.io.DrainException;
import org.adealsystems.platform.io.compression.Compression;
import org.apache.commons.csv.CSVFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public class EntryCsvDrain extends AbstractCsvDrain<Entry> {
    // be aware that an actual, non-test implementation would NOT
    // leave csvFormat as a c'tor argument.
    //
    // Instead, it would call the super c'tor with a proper format
    // matching the implementation of the getValue method.

    public EntryCsvDrain(OutputStream outputStream, CSVFormat csvFormat) throws IOException {
        super(outputStream, csvFormat);
    }

    public EntryCsvDrain(OutputStream outputStream, CSVFormat csvFormat, Compression compression) throws IOException {
        super(outputStream, csvFormat, compression);
    }

    public EntryCsvDrain(BufferedWriter writer, CSVFormat csvFormat) throws IOException {
        super(writer, csvFormat);
    }

    @Override
    public String getValue(Entry entry, String columnName) {
        Objects.requireNonNull(entry, "entry must not be null!");
        Objects.requireNonNull(columnName, "columnName must not be null!");
        switch (columnName) {
            case "key":
                return entry.getKey();
            case "value":
                return entry.getValue();
            default:
                throw new DrainException("Unknown column name '" + columnName + "'!");
        }
    }
}
