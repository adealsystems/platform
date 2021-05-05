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

package org.adealsystems.platform.io.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.WellException;
import org.adealsystems.platform.io.compression.Compression;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class JsonlWell<E> implements Well<E> {
    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();
    private final ObjectMapper objectMapper;
    private final Class<E> clazz;
    private BufferedReader reader;
    private boolean consumed;

    public JsonlWell(Class<E> clazz, InputStream inputStream)
            throws IOException {
        this(clazz, inputStream, Compression.NONE, DEFAULT_OBJECT_MAPPER);
    }

    public JsonlWell(Class<E> clazz, InputStream inputStream, Compression compression)
            throws IOException {
        this(clazz, Compression.createReader(inputStream, compression), DEFAULT_OBJECT_MAPPER);
    }

    public JsonlWell(Class<E> clazz, InputStream inputStream, ObjectMapper objectMapper)
            throws IOException {
        this(clazz, inputStream, Compression.NONE, objectMapper);
    }

    public JsonlWell(Class<E> clazz, InputStream inputStream, Compression compression, ObjectMapper objectMapper)
            throws IOException {
        this(clazz, Compression.createReader(inputStream, compression), objectMapper);
    }

    /*
     * This constructor is private so we can be sure the writer is a
     * BufferedWriter with correct charset, i.e. UTF-8.
     */
    private JsonlWell(Class<E> clazz, BufferedReader reader, ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null!");
        this.reader = reader; // private c'tor, already checked against null
        this.clazz = Objects.requireNonNull(clazz, "clazz must not be null!");
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    @Override
    public void close() {
        Throwable throwable = null;
        try {
            reader.close();
        } catch (Throwable t) {
            throwable = t;
        }
        reader = null;
        if (throwable != null) {
            throw new WellException("Failed to close stream!", throwable);
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

    private E readNextValue() {
        if (reader == null) {
            throw new IllegalStateException("Well was already closed!");
        }
        try {
            String line = reader.readLine();
            if (line == null) {
                close();
                return null;
            }
            return objectMapper.readValue(line, clazz);
        } catch (IOException e) {
            throw new WellException("Failed to read from stream!", e);
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
