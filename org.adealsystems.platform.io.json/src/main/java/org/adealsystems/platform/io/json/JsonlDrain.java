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
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

/**
 * JSON Lines (.jsonl) Drain
 * <p>
 * https://jsonlines.org/
 *
 * @param <E> the type this Drain can handle.
 */
public class JsonlDrain<E> implements Drain<E> {
    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();
    private final ReentrantLock lock = new ReentrantLock();
    private final ObjectMapper objectMapper;
    private BufferedWriter writer;

    public JsonlDrain(OutputStream outputStream)
            throws IOException {
        this(outputStream, Compression.NONE, DEFAULT_OBJECT_MAPPER);
    }

    public JsonlDrain(OutputStream outputStream, Compression compression)
            throws IOException {
        this(createWriter(outputStream, compression), DEFAULT_OBJECT_MAPPER);
    }

    public JsonlDrain(OutputStream outputStream, ObjectMapper objectMapper)
            throws IOException {
        this(outputStream, Compression.NONE, objectMapper);
    }

    public JsonlDrain(OutputStream outputStream, Compression compression, ObjectMapper objectMapper)
            throws IOException {
        this(createWriter(outputStream, compression), objectMapper);
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

        lock.lock();
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
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("PMD.AvoidCatchingNPE")
    public void addAll(Collection<E> collection) {
        Objects.requireNonNull(collection, "collection must not be null!");
        lock.lock();
        try {
            try {
                if (collection.contains(null)) {
                    throw new IllegalArgumentException("collection must not contain null!");
                }
            } catch (NullPointerException ex) {
                // ignore
                //
                // this can happen if the collection does not support null values, but
                // usually doesn't.
                //
                // Someone considered this acceptable (optional) behaviour while defining
                // the Collection API... instead of just, you know, returning
                // false if the Collection does not support null values.
                // Which would be the obviously correct answer.
                //
                // But I digress...
            }

            for (E entry : collection) {
                add(entry);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public void close() throws Exception {
        lock.lock();
        try {
            if (writer == null) {
                return;
            }
            BufferedWriter temp = writer;
            writer = null;
            temp.flush();
            temp.close();
        } finally {
            lock.unlock();
        }
    }

    public enum Compression {
        NONE,
        GZIP,
        BZIP,
    }

    private static BufferedWriter createWriter(OutputStream outputStream, Compression compression) throws IOException {
        Objects.requireNonNull(outputStream, "outputStream must not be null!");
        Objects.requireNonNull(compression);
        switch (compression) {
            case NONE:
                return new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            case GZIP:
                return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(outputStream), StandardCharsets.UTF_8));
            case BZIP:
                return new BufferedWriter(new OutputStreamWriter(new BZip2CompressorOutputStream(outputStream), StandardCharsets.UTF_8));
            default:
                throw new IllegalArgumentException("Compression " + compression + " isn't (yet) supported!");
        }
    }
}
