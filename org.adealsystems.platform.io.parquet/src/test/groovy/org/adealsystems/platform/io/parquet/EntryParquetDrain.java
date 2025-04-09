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

package org.adealsystems.platform.io.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;

import java.io.IOException;
import java.util.Objects;

public class EntryParquetDrain extends AbstractParquetDrain<Entry> {
    // be aware that an actual, non-test implementation would NOT
    // leave schema and builder as a c'tor argument.
    //
    // Instead, it would call the super c'tor with a proper schema
    // and builder matching the implementation of the convert method.
    public EntryParquetDrain(Schema schema, AvroParquetWriter.Builder<GenericRecord> builder) throws IOException {
        super(schema, builder);
    }

    @Override
    protected GenericRecord convert(Entry entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        GenericRecord record = createRecord();
        record.put(EntryAvroConstants.ID_FIELD, entry.getId());
        record.put(EntryAvroConstants.VALUE_FIELD, entry.getValue());
        return record;
    }
}
