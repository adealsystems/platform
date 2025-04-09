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

import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.WellException;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public abstract class AbstractParquetWell<E> implements Well<E> {
    private ParquetReader<GenericRecord> parquetReader;
    private boolean consumed;

    protected AbstractParquetWell(AvroParquetReader.Builder<GenericRecord> builder) throws IOException {
        Objects.requireNonNull(builder, "builder must not be null!");
        parquetReader = builder.build();
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    @Override
    public void close() {
        if (parquetReader == null) {
            return;
        }
        Throwable throwable = null;
        try {
            parquetReader.close();
        } catch (Throwable t) {
            throwable = t;
        }
        parquetReader = null;
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

    private E readNextValue() {
        if (parquetReader == null) {
            throw new WellException("Well was already closed!");
        }
        GenericRecord genericRecord;
        try {
            genericRecord = parquetReader.read();
            if (genericRecord == null) {
                close();
                return null;
            }
        } catch (Throwable t) {
            throw new WellException("Exception while reading record!", t);
        }

        try {
            return convert(genericRecord);
        } catch (Throwable t) {
            throw new WellException("Exception while converting record!", t);
        }
     }

    private class WellIterator implements Iterator<E> {
        private E nextValue;

        WellIterator() {
            nextValue = readNextValue();
        }

        @Override
        public boolean hasNext() {
            return nextValue != null;
        }

        @Override
        public E next() {
            if (nextValue == null) {
                throw new NoSuchElementException();
            }
            E result = nextValue;
            nextValue = readNextValue();
            return result;
        }
    }
}
