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

package org.adealsystems.platform.io.line;

import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.WellException;
import org.adealsystems.platform.io.compression.Compression;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class LineWell implements Well<String> {
    private BufferedReader reader;
    private boolean consumed;

    public LineWell(InputStream inputStream)
        throws IOException {
        this(inputStream, Compression.NONE);
    }

    public LineWell(InputStream inputStream, Compression compression)
        throws IOException {
        this(Objects.requireNonNull(compression, "compression must not be null!").createReader(inputStream));
    }


    /*
     * This constructor is package-private so we can be sure the reader is a
     * BufferedReader with correct charset, i.e. UTF-8.
     */
    LineWell(BufferedReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader must not be null!");
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    @Override
    public void close() {
        if (reader == null) {
            return;
        }
        Throwable throwable = null;
        try {
            reader.close();
        } catch (Throwable t) {
            throwable = t;
        }
        reader = null;
        if (throwable != null) {
            throw new WellException("Exception while closing stream!", throwable);
        }
    }

    @Override
    public Iterator<String> iterator() {
        if (consumed) {
            throw new WellException("A well can only be iterated once!");
        }
        consumed = true;
        return new WellIterator();
    }

    private String readNextValue() {
        if (reader == null) {
            throw new WellException("Well was already closed!");
        }
        try {
            String line = reader.readLine();
            if (line == null) {
                close();
                return null;
            }
            return line;
        } catch (IOException e) {
            throw new WellException("Exception while reading line!", e);
        }
    }

    private class WellIterator implements Iterator<String> {
        private String nextValue;

        WellIterator() {
            nextValue = readNextValue();
        }

        @Override
        public boolean hasNext() {
            return nextValue != null;
        }

        @Override
        public String next() {
            if (nextValue == null) {
                throw new NoSuchElementException();
            }
            String result = nextValue;
            nextValue = readNextValue();
            return result;
        }
    }
}
