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

package org.adealsystems.platform.io.csv;

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.DrainException;
import org.adealsystems.platform.io.compression.Compression;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public abstract class AbstractCsvDrain<E> implements Drain<E> {
    private final String[] header;
    private CSVPrinter printer;

    protected AbstractCsvDrain(OutputStream outputStream, CSVFormat csvFormat)
            throws IOException {
        this(outputStream, csvFormat, Compression.NONE);
    }

    protected AbstractCsvDrain(OutputStream outputStream, CSVFormat csvFormat, Compression compression)
            throws IOException {
        this(Compression.createWriter(outputStream, compression), csvFormat);
    }

    /*
     * This constructor is private so we can be sure the writer is a
     * BufferedWriter with correct charset, i.e. UTF-8.
     */
    private AbstractCsvDrain(BufferedWriter writer, CSVFormat csvFormat)
            throws IOException {
        this.header = resolveHeader(csvFormat);
        this.printer = csvFormat.print(writer); // private c'tor, already checked against null
    }

    private static String[] resolveHeader(CSVFormat csvFormat) {
        Objects.requireNonNull(csvFormat, "csvFormat must not be null!");
        return Objects.requireNonNull(csvFormat.getHeader(), "csvFormat does not contain a header!");
    }

    protected abstract Object getValue(E entry, String columnName);

    @Override
    public void add(E entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        if (printer == null) {
            throw new IllegalStateException("Drain was already closed!");
        }

        Object[] recordValues = new Object[header.length];
        for (int i = 0; i < header.length; i++) {
            recordValues[i] = getValue(entry, header[i]);
        }
        try {
            printer.printRecord(recordValues);
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
    public void close() {
        if (printer == null) {
            return;
        }
        CSVPrinter temp = printer;
        printer = null;
        try {
            temp.flush();
            temp.close();
        }
        catch(IOException ex) {
            throw new DrainException("Exception while closing stream!", ex);
        }
    }
}
