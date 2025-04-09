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

package org.adealsystems.platform.io.avro;

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.DrainException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public abstract class AbstractAvroDrain<E> implements Drain<E> {
    private final Schema schema;
    private DataFileWriter<GenericRecord> dataFileWriter;

    @SuppressWarnings("PMD.CloseResource")
    protected AbstractAvroDrain(Schema schema, CodecFactory codecFactory, OutputStream outputStream)
        throws IOException {
        this.schema = Objects.requireNonNull(schema, "schema must not be null!");
        Objects.requireNonNull(outputStream, "outputStream must not be null!");
        Objects.requireNonNull(codecFactory, "codecFactory must not be null!");

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter).setCodec(codecFactory);
        this.dataFileWriter = dataFileWriter.create(schema, outputStream);
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
        if (dataFileWriter == null) {
            throw new DrainException("Drain was already closed!");
        }
        try {
            dataFileWriter.append(convert(entry));
        } catch (Throwable t) {
            throw new DrainException("Exception while appending entry!", t);
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
        if (dataFileWriter == null) {
            return;
        }

        Throwable throwable = null;
        try {
            dataFileWriter.flush();
            dataFileWriter.close();
        } catch (Throwable t) {
            throwable = t;
        }
        dataFileWriter = null;
        if (throwable != null) {
            throw new DrainException("Exception while closing dataFileWriter!", throwable);
        }
    }
}
