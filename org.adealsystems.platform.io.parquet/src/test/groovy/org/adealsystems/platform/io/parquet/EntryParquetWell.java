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

package org.adealsystems.platform.io.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;

public class EntryParquetWell extends AbstractParquetWell<Entry> {
    // be aware that an actual, non-test implementation would NOT
    // leave builder as a c'tor argument.
    //
    // Instead, it would call the super c'tor with a proper builder
    // matching the implementation of the convert method.

    public EntryParquetWell(AvroParquetReader.Builder<GenericRecord> builder) throws IOException {
        super(builder);
    }

    @Override
    protected Entry convert(GenericRecord record) {
        Entry result = new Entry();
        result.setId((Integer) record.get(EntryAvroConstants.ID_FIELD));

        Utf8 value = (Utf8) record.get(EntryAvroConstants.VALUE_FIELD);
        if(value != null) {
            result.setValue(value.toString()); // that's really correct
        }
        return result;
    }
}
