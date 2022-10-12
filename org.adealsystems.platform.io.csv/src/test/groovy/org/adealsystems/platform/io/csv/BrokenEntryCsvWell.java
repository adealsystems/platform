/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

import org.adealsystems.platform.io.compression.Compression;
import org.apache.commons.csv.CSVFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class BrokenEntryCsvWell extends AbstractCsvWell<BrokenEntry> {
    // be aware that an actual, non-test implementation would NOT
    // leave csvFormat as a c'tor argument.
    //
    // Instead, it would call the super c'tor with a proper format
    // matching the implementation of the setValue method.

    public BrokenEntryCsvWell(InputStream inputStream, CSVFormat csvFormat) throws IOException {
        super(BrokenEntry.class, inputStream, csvFormat);
    }

    public BrokenEntryCsvWell(InputStream inputStream, CSVFormat csvFormat, Compression compression) throws IOException {
        super(BrokenEntry.class, inputStream, csvFormat, compression);
    }

    public BrokenEntryCsvWell(BufferedReader reader, CSVFormat csvFormat) throws IOException {
        super(BrokenEntry.class, reader, csvFormat);
    }

    @Override
    public void setValue(BrokenEntry entry, String columnName, String value) {
        Objects.requireNonNull(entry, "entry must not be null!");
        Objects.requireNonNull(columnName, "columnName must not be null!");
        switch (columnName) {
            case "key":
                entry.setKey(value);
                break;
            case "value":
                entry.setValue(value);
                break;
            default:
                // ignore
                // other implementations might want to be harsher
                break;
        }
    }
}
