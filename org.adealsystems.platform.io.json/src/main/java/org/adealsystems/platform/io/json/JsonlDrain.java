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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.DrainException;
import org.adealsystems.platform.io.compression.Compression;

import java.io.BufferedWriter;
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
    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();
    private final ObjectMapper objectMapper;
    private BufferedWriter writer;

    public JsonlDrain(OutputStream outputStream)
            throws IOException {
        this(outputStream, Compression.NONE, DEFAULT_OBJECT_MAPPER);
    }

    public JsonlDrain(OutputStream outputStream, Compression compression)
            throws IOException {
        this(Compression.createWriter(outputStream, compression), DEFAULT_OBJECT_MAPPER);
    }

    public JsonlDrain(OutputStream outputStream, ObjectMapper objectMapper)
            throws IOException {
        this(outputStream, Compression.NONE, objectMapper);
    }

    public JsonlDrain(OutputStream outputStream, Compression compression, ObjectMapper objectMapper)
            throws IOException {
        this(Compression.createWriter(outputStream, compression), objectMapper);
    }

    /*
     * This constructor is private so we can be sure the writer is a
     * BufferedWriter with correct charset, i.e. UTF-8.
     */
    private JsonlDrain(BufferedWriter writer, ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null!");
        this.writer = writer; // private c'tor, already checked against null
        // ensure that objectMapper is not pretty-printing
        if (objectMapper.isEnabled(SerializationFeature.INDENT_OUTPUT)) {
            throw new IllegalArgumentException("objectMapper must not have INDENT_OUTPUT feature enabled!");
        }
    }

    @Override
    public void add(E entry) {
        Objects.requireNonNull(entry, "entry must not be null!");

        try {
            if (writer == null) {
                throw new IllegalStateException("Drain was already closed!");
            }
            String json = objectMapper.writeValueAsString(entry);
            writer.write(json);
            writer.write('\n');
        } catch (JsonProcessingException e) {
            throw new DrainException("Failed to write entry as JSON!", e);
        } catch (IOException e) {
            throw new DrainException("Failed to write to stream!", e);
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
    @SuppressWarnings("PMD.CloseResource")
    public void close() throws Exception {
        if (writer == null) {
            return;
        }
        BufferedWriter temp = writer;
        writer = null;
        temp.flush();
        temp.close();
    }
}
