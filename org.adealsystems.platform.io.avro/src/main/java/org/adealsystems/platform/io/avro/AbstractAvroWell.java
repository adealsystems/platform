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

package org.adealsystems.platform.io.avro;

import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.WellException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public abstract class AbstractAvroWell<E> implements Well<E> {
    private final Schema schema;
    private DataFileStream<GenericRecord> dataFileStream;
    private boolean consumed;

    protected AbstractAvroWell(Schema schema, InputStream inputStream) throws IOException {
        this.schema = Objects.requireNonNull(schema, "schema must not be null!");
        Objects.requireNonNull(inputStream, "inputStream must not be null!");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        dataFileStream = new DataFileStream<>(inputStream, datumReader);
    }

    protected Schema getSchema() {
        return schema;
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    @Override
    public void close() {
        if (dataFileStream == null) {
            return;
        }
        Throwable throwable = null;
        try {
            dataFileStream.close();
        } catch (Throwable t) {
            throwable = t;
        }
        dataFileStream = null;
        if (throwable != null) {
            throw new WellException("Exception while closing well!", throwable);
        }
    }

    @Override
    public Iterator<E> iterator() {
        if (consumed) {
            throw new WellException("A well can only be iterated once!");
        }
        consumed = true;
        return new WellIterator();
    }

    protected abstract E convert(GenericRecord record);

    private class WellIterator implements Iterator<E> {
        private final Iterator<GenericRecord> iterator;

        WellIterator() {
            this.iterator = dataFileStream;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public E next() {
            if (dataFileStream == null) {
                throw new WellException("Well was already closed!");
            }
            try {
                if (!iterator.hasNext()) {
                    throw new NoSuchElementException();
                }

                return convert(iterator.next());
            } catch (NoSuchElementException ex) { // NOPMD
                throw ex;
            } catch (Throwable t) {
                throw new WellException("Exception while reading record!", t);
            }
        }
    }
}
