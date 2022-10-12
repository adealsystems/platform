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

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.DrainException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.Objects;

public abstract class AbstractParquetDrain<E> implements Drain<E> {
    private final Schema schema;
    private ParquetWriter<GenericRecord> parquetWriter;

    @SuppressWarnings("PMD.CloseResource")
    protected AbstractParquetDrain(Schema schema, AvroParquetWriter.Builder<GenericRecord> builder)
        throws IOException {
        this.schema = Objects.requireNonNull(schema, "schema must not be null!");
        builder.withWriteMode(ParquetFileWriter.Mode.OVERWRITE);
        builder.withSchema(schema);
        parquetWriter = builder.build();
    }

    protected Schema getSchema() {
        return schema;
    }

    protected GenericRecord createRecord() {
        return new GenericData.Record(schema);
    }

    protected abstract GenericRecord convert(E entry);

    @Override
    public void add(E entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        if (parquetWriter == null) {
            throw new DrainException("Drain was already closed!");
        }
        try {
            parquetWriter.write(convert(entry));
        } catch (Throwable t) {
            throw new DrainException("Exception while writing entry!", t);
        }
    }

    @Override
    public void addAll(Iterable<E> entries) {
        Objects.requireNonNull(entries, "entries must not be null!");
        for (E entry : entries) {
            add(Objects.requireNonNull(entry, "entries must not contain null!"));
        }
    }

    @Override
    public void close() {
        if (parquetWriter == null) {
            return;
        }

        Throwable throwable = null;
        try {
            parquetWriter.close();
        } catch (Throwable t) {
            throwable = t;
        }
        parquetWriter = null;
        if (throwable != null) {
            throw new DrainException("Exception while closing dataFileWriter!", throwable);
        }
    }
}
