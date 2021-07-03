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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class AbstractCsvDrain<E> implements Drain<E> {
    private final String[] header;
    private final List<String> headerList;
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
     * This constructor is package-private so we can be sure the writer is a
     * BufferedWriter with correct charset, i.e. UTF-8.
     */
    AbstractCsvDrain(BufferedWriter writer, CSVFormat csvFormat)
        throws IOException {
        this.header = resolveHeader(csvFormat);
        this.headerList = Collections.unmodifiableList(Arrays.asList(header));
        this.printer = csvFormat.print(writer);
    }

    private static String[] resolveHeader(CSVFormat csvFormat) {
        Objects.requireNonNull(csvFormat, "csvFormat must not be null!");
        return Objects.requireNonNull(csvFormat.getHeader(), "csvFormat does not contain a header!");
    }

    public List<String> getHeaders() {
        return headerList;
    }

    public abstract String getValue(E entry, String columnName);

    @Override
    public void add(E entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        if (printer == null) {
            throw new DrainException("Drain was already closed!");
        }

        Object[] recordValues = new Object[header.length];
        for (int i = 0; i < header.length; i++) {
            recordValues[i] = getValue(entry, header[i]);
        }
        try {
            printer.printRecord(recordValues);
        } catch (Throwable t) {
            throw new DrainException("Exception while printing record!", t);
        }
    }

    @Override
    public void addAll(Iterable<E> entries) {
        Objects.requireNonNull(entries, "entries must not be null!");
        if (printer == null) {
            throw new DrainException("Drain was already closed!");
        }

        Object[] recordValues = new Object[header.length];
        for (E entry : entries) {
            Objects.requireNonNull(entry, "entries must not contain null!");
            for (int i = 0; i < header.length; i++) {
                recordValues[i] = getValue(entry, header[i]);
            }
            try {
                printer.printRecord(recordValues);
            } catch (Throwable t) {
                throw new DrainException("Exception while printing record!", t);
            }
        }
    }

    @Override
    public void close() {
        if (printer == null) {
            return;
        }

        Throwable throwable = null;
        try {
            printer.flush();
            printer.close();
        } catch (Throwable t) {
            throwable = t;
        }
        printer = null;
        if (throwable != null) {
            throw new DrainException("Exception while closing printer!", throwable);
        }
    }
}
