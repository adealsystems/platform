/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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
import org.apache.commons.io.input.BOMInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public abstract class AbstractCsvWell<E> implements Well<E> {
    private final Class<E> clazz;
    private final CSVFormat csvFormat;
    private CSVParser parser;
    private final String[] header;
    private final List<String> headerList;
    private final Iterator<CSVRecord> parserIterator;
    private boolean consumed;

    protected AbstractCsvWell(Class<E> clazz, InputStream inputStream, CSVFormat csvFormat)
        throws IOException {
        this(clazz, inputStream, csvFormat, Compression.NONE);
    }

    protected AbstractCsvWell(Class<E> clazz, InputStream inputStream, CSVFormat csvFormat, Compression compression)
        throws IOException {
        this(clazz, Objects.requireNonNull(compression, "compression must not be null!").createReader(
            new BOMInputStream(inputStream)), csvFormat);
    }

    /*
     * This constructor is package-private so we can be sure the reader is a
     * BufferedReader with correct charset, i.e. UTF-8.
     */
    AbstractCsvWell(Class<E> clazz, BufferedReader reader, CSVFormat csvFormat)
        throws IOException {
        this.clazz = Objects.requireNonNull(clazz, "clazz must not be null!");
        Objects.requireNonNull(reader, "reader must not be null!");
        this.csvFormat = Objects.requireNonNull(csvFormat, "csvFormat must not be null!");
        this.parser = csvFormat.parse(reader);
        this.header = resolveHeader(parser);
        this.headerList = Collections.unmodifiableList(Arrays.asList(header));
        this.parserIterator = parser.iterator();
    }

    private static String[] resolveHeader(CSVParser csvParser) {
        Objects.requireNonNull(csvParser, "csvParser must not be null!");
        List<String> headerNames = csvParser.getHeaderNames();
        return headerNames.toArray(new String[0]);
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    public CSVFormat getCSVFormat() {
        return csvFormat;
    }

    public List<String> getHeaders() {
        return headerList;
    }

    public abstract void setValue(E entry, String columnName, String value);

    @Override
    public void close() {
        if (parser == null) {
            return;
        }
        Throwable throwable = null;
        try {
            parser.close();
        } catch (Throwable t) {
            throwable = t;
        }
        parser = null;
        if (throwable != null) {
            throw new WellException("Exception while closing parser!", throwable);
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
        if (parser == null) {
            throw new WellException("Well was already closed!");
        }
        E result;
        try {
            result = clazz.getDeclaredConstructor().newInstance();
        } catch (Throwable t) {
            throw new WellException("Exception while creating instance of class " + clazz.getName() + "!", t);
        }
        try {
            if (!parserIterator.hasNext()) {
                throw new NoSuchElementException();
            }

            CSVRecord record = parserIterator.next();
            for (String headerName : header) {
                setValue(result, headerName, record.get(headerName));
            }
        } catch (NoSuchElementException ex) { // NOPMD
            throw ex;
        } catch (Throwable t) {
            throw new WellException("Exception while reading record!", t);
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
