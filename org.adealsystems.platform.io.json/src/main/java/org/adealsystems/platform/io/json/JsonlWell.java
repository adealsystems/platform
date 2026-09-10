/*
 * Copyright 2020-2026 ADEAL Systems GmbH
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

import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.WellException;
import org.adealsystems.platform.io.compression.Compression;
import org.adealsystems.platform.io.line.LineWell;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Objects;

public class JsonlWell<E> implements Well<E> {
    private static final JsonMapper DEFAULT_JSON_MAPPER =
        JsonMapper.builder()
            .build();

    private final JsonMapper jsonMapper;
    private final Class<E> clazz;
    private Well<String> stringWell;
    private boolean consumed;

    public JsonlWell(Class<E> clazz, InputStream inputStream)
        throws IOException {
        this(clazz, inputStream, Compression.NONE, DEFAULT_JSON_MAPPER);
    }

    public JsonlWell(Class<E> clazz, InputStream inputStream, Compression compression)
        throws IOException {
        this(clazz, inputStream, compression, DEFAULT_JSON_MAPPER);
    }

    public JsonlWell(Class<E> clazz, InputStream inputStream, JsonMapper jsonMapper)
        throws IOException {
        this(clazz, inputStream, Compression.NONE, jsonMapper);
    }

    public JsonlWell(Class<E> clazz, InputStream inputStream, Compression compression, JsonMapper jsonMapper)
        throws IOException {
        this(clazz, new LineWell(inputStream, compression), jsonMapper);
    }

    public JsonlWell(Class<E> clazz, Well<String> stringWell) {
        this(clazz, stringWell, DEFAULT_JSON_MAPPER);
    }

    public JsonlWell(Class<E> clazz, Well<String> stringWell, JsonMapper jsonMapper) {
        this.clazz = Objects.requireNonNull(clazz, "clazz must not be null!");
        this.stringWell = Objects.requireNonNull(stringWell, "stringWell must not be null!");
        this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper must not be null!");
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    @Override
    public void close() {
        if (stringWell == null) {
            return;
        }
        Throwable throwable = null;
        try {
            stringWell.close();
        } catch (Throwable t) {
            throwable = t;
        }
        stringWell = null;
        if (throwable != null) {
            throw new WellException("Exception while closing well!", throwable);
        }
    }

    @SuppressWarnings("NullableProblems") // as if
    @Override
    public Iterator<E> iterator() {
        if (consumed) {
            throw new WellException("A well can only be iterated once!");
        }
        consumed = true;
        return new WellIterator();
    }

    private class WellIterator implements Iterator<E> {
        private final Iterator<String> iterator;

        WellIterator() {
            this.iterator = stringWell.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public E next() {
            if (stringWell == null) {
                throw new WellException("Well was already closed!");
            }
            String line = iterator.next();
            try {
                return jsonMapper.readValue(line, clazz);
            } catch (JacksonException e) {
                throw new WellException("Failed to parse JSON!", e);
            }
        }
    }
}
