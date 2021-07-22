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

package org.adealsystems.platform.io.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.InputStream;

public class EntryAvroWell extends AbstractAvroWell<Entry> {
    // be aware that an actual, non-test implementation would NOT
    // leave schema as a c'tor argument.
    //
    // Instead, it would call the super c'tor with a proper schema
    // matching the implementation of the convert method.

    public EntryAvroWell(Schema schema, InputStream inputStream) throws IOException {
        super(schema, inputStream);
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
