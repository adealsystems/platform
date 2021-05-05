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

import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.WellException;
import org.adealsystems.platform.io.compression.Compression;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public abstract class AbstractCsvWell<E> implements Well<E> {
    private final Class<E> clazz;
    private CSVParser parser;
    private final String[] header;
    private final Iterator<CSVRecord> parserIterator;
    private boolean consumed;

    protected AbstractCsvWell(Class<E> clazz, InputStream inputStream, CSVFormat csvFormat)
            throws IOException {
        this(clazz, inputStream, csvFormat, Compression.NONE);
    }

    protected AbstractCsvWell(Class<E> clazz, InputStream inputStream, CSVFormat csvFormat, Compression compression)
            throws IOException {
        this(clazz, Compression.createReader(inputStream, compression), csvFormat);
    }

    /*
     * This constructor is private so we can be sure the writer is a
     * BufferedWriter with correct charset, i.e. UTF-8.
     */
    private AbstractCsvWell(Class<E> clazz, BufferedReader reader, CSVFormat csvFormat)
            throws IOException {
        this.clazz = Objects.requireNonNull(clazz, "clazz must not be null!");
        this.parser = csvFormat.parse(reader); // private c'tor, already checked against null
        this.header = resolveHeader(parser);
        this.parserIterator = parser.iterator();
    }

    private static String[] resolveHeader(CSVParser csvParser) {
        Objects.requireNonNull(csvParser, "csvParser must not be null!");
        List<String> headerNames = csvParser.getHeaderNames();
        // TODO: check
        return headerNames.toArray(new String[0]);
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    protected abstract void setValue(E entry, String columnName, String value);

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public void close() throws Exception {
        if (parser == null) {
            return;
        }
        CSVParser temp = parser;
        parser = null;
        temp.close();
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
        if (parser == null) {
            throw new IllegalStateException("Well was already closed!");
        }
        if (!parserIterator.hasNext()) {
            throw new NoSuchElementException();
        }

        E result;
        try {
            result = clazz.newInstance();
        } catch (Throwable t) {
            throw new WellException("Failed to create result!", t);
        }
        CSVRecord record = parserIterator.next();
        for (String headerName : header) {
            setValue(result, headerName, record.get(headerName));
        }
        if (!parserIterator.hasNext()) {
            try {
                close();
            } catch (Exception e) {
                throw new WellException("Failed to close stream!", e);
            }
        }
        return result;
    }

    private class WellIterator implements Iterator<E> {
        WellIterator() {
        }

        @Override
        public boolean hasNext() {
            return parserIterator.hasNext();
        }

        @Override
        public E next() {
            return readNextValue();
        }
    }
}
