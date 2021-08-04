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

package org.adealsystems.platform.io.parquet;

import org.apache.avro.Schema;

public interface EntryAvroConstants {
    /**
     * Initially obtained via ReflectData.get().getSchema(Entry.class);
     *
     * This schema does - by default - not allow null for Entry.value.
     */
    String SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"Entry\",\"namespace\":\"org.adealsystems.platform.io.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"string\"}]}";
    Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

    /**
     * This schema allows null for Entry.value.
     */
    String RELAXED_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"Entry\",\"namespace\":\"org.adealsystems.platform.io.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"value\",\"type\":[\"string\",\"null\"]}]}";
    Schema RELAXED_SCHEMA = new Schema.Parser().parse(RELAXED_SCHEMA_STRING);

    String ID_FIELD = "id";
    String VALUE_FIELD = "value";
}
