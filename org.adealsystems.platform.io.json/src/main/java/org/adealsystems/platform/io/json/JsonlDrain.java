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

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.DrainException;
import org.adealsystems.platform.io.compression.Compression;
import org.adealsystems.platform.io.line.LineDrain;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * JSON Lines (.jsonl) Drain
 * <p>
 * https://jsonlines.org/
 *
 * @param <E> the type this Drain can handle.
 */
public class JsonlDrain<E> implements Drain<E> {
    private static final JsonMapper DEFAULT_JSON_MAPPER =
        JsonMapper.builder()
            .build();

    private final JsonMapper jsonMapper;
    private Drain<String> stringDrain;

    public JsonlDrain(OutputStream outputStream)
        throws IOException {
        this(outputStream, Compression.NONE, DEFAULT_JSON_MAPPER);
    }

    public JsonlDrain(OutputStream outputStream, Compression compression)
        throws IOException {
        this(outputStream, compression, DEFAULT_JSON_MAPPER);
    }

    public JsonlDrain(OutputStream outputStream, JsonMapper jsonMapper)
        throws IOException {
        this(outputStream, Compression.NONE, jsonMapper);
    }

    public JsonlDrain(OutputStream outputStream, Compression compression, JsonMapper jsonMapper)
        throws IOException {
        this(new LineDrain(outputStream, compression), jsonMapper);
    }

    public JsonlDrain(Drain<String> stringDrain) {
        this(stringDrain, DEFAULT_JSON_MAPPER);
    }

    public JsonlDrain(Drain<String> stringDrain, JsonMapper jsonMapper) {
        this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper must not be null!");
        this.stringDrain = Objects.requireNonNull(stringDrain, "stringDrain must not be null!");
        // ensure that JsonMapper is not pretty-printing
        if (jsonMapper.isEnabled(SerializationFeature.INDENT_OUTPUT)) {
            throw new IllegalArgumentException("jsonMapper must not have INDENT_OUTPUT feature enabled!");
        }
    }

    @Override
    public void add(E entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        if (stringDrain == null) {
            throw new DrainException("Drain was already closed!");
        }

        try {
            stringDrain.add(jsonMapper.writeValueAsString(entry));
        } catch (JacksonException e) {
            throw new DrainException("Failed to write entry as JSON!", e);
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
        if (stringDrain == null) {
            return;
        }
        Throwable throwable = null;
        try {
            stringDrain.close();
        } catch (Throwable t) {
            throwable = t;
        }
        stringDrain = null;
        if (throwable != null) {
            throw new DrainException("Exception while closing drain!", throwable);
        }
    }
}
