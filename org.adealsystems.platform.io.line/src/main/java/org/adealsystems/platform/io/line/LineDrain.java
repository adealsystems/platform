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

package org.adealsystems.platform.io.line;

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.DrainException;
import org.adealsystems.platform.io.compression.Compression;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public class LineDrain implements Drain<String> {
    private BufferedWriter writer;

    public LineDrain(OutputStream outputStream)
        throws IOException {
        this(outputStream, Compression.NONE);
    }

    public LineDrain(OutputStream outputStream, Compression compression)
        throws IOException {
        this(Objects.requireNonNull(compression, "compression must not be null!").createWriter(outputStream));
    }

    /*
     * This constructor is package-private so we can be sure the writer is a
     * BufferedWriter with correct charset, i.e. UTF-8.
     */
    LineDrain(BufferedWriter writer) {
        this.writer = Objects.requireNonNull(writer, "writer must not be null!");
    }

    @Override
    public void add(String entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        try {
            if (writer == null) {
                throw new DrainException("Drain was already closed!");
            }
            writer.write(entry);
            writer.write('\n');
        } catch (IOException e) {
            throw new DrainException("Exception while writing line!", e);
        }
    }

    @Override
    public void addAll(Iterable<String> entries) {
        Objects.requireNonNull(entries, "entries must not be null!");
        for (String entry : entries) {
            add(Objects.requireNonNull(entry, "entries must not contain null!"));
        }
    }

    @Override
    public void close() {
        if (writer == null) {
            return;
        }
        Throwable throwable = null;
        try {
            writer.flush();
            writer.close();
        } catch (Throwable t) {
            throwable = t;
        }
        writer = null;
        if (throwable != null) {
            throw new DrainException("Exception while closing stream!", throwable);
        }
    }
}
