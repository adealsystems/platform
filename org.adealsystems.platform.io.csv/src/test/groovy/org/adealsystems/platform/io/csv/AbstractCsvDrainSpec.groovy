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

package org.adealsystems.platform.io.csv

import org.adealsystems.platform.io.DrainException
import org.adealsystems.platform.io.compression.Compression
import org.apache.commons.csv.CSVFormat
import spock.lang.Specification

class AbstractCsvDrainSpec extends Specification {
    final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
        .withHeader("key", "value")
        .withDelimiter(';' as char)

    def 'adding to the drain with compression #compression works'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT, compression)

        when:
        instance.add(new Entry("Key 1", "Value 1"))
        and:
        instance.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        and:
        instance.close()

        and:
        List<String> lines = readLines(bos.toByteArray(), compression)

        then:
        lines == ['key;value', 'Key 1;Value 1', 'Key 2;Value 2', 'Key 3;Value 3']

        where:
        compression << [
            Compression.NONE,
            Compression.GZIP,
            Compression.BZIP,
        ]
    }

    def 'add(..) throws exception if already closed'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT)

        when:
        instance.close()
        and:
        instance.add(new Entry("Key 1", "Value 1"))

        then:
        IllegalStateException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'addAll(..) throws exception if already closed'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT)

        when:
        instance.close()
        and:
        instance.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])

        then:
        IllegalStateException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'add(null) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT)

        when:
        instance.add(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entry must not be null!"
    }

    def 'addAll(null) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT)

        when:
        instance.addAll(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not be null!"
    }

    def 'addAll([null]) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT)

        when:
        instance.addAll([null])

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not contain null!"
    }

    private static class Entry {
        String key
        String value

        @SuppressWarnings('unused')
        Entry() {
        }

        Entry(String key, String value) {
            this.key = key
            this.value = value
        }

        boolean equals(o) {
            if (this.is(o)) return true
            if (getClass() != o.class) return false

            Entry entry = (Entry) o

            if (key != entry.key) return false
            if (value != entry.value) return false

            return true
        }

        int hashCode() {
            int result
            result = (key != null ? key.hashCode() : 0)
            result = 31 * result + (value != null ? value.hashCode() : 0)
            return result
        }

        @Override
        String toString() {
            return "Entry{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}'
        }
    }

    private static List<String> readLines(byte[] bytes, Compression compression) {
        Objects.requireNonNull(compression, "compression must not be null!")
        return Compression.createReader(new ByteArrayInputStream(bytes), compression).readLines()
    }

    static class EntryCsvDrain extends AbstractCsvDrain<Entry> {
        // be aware that an actual, non-test implementation would NOT
        // leave csvFormat as a c'tor argument.
        //
        // Instead, it would call the super c'tor with a proper format
        // matching the implementation of the getValue method.

        EntryCsvDrain(OutputStream outputStream, CSVFormat csvFormat) throws IOException {
            super(outputStream, csvFormat)
        }

        EntryCsvDrain(OutputStream outputStream, CSVFormat csvFormat, Compression compression) throws IOException {
            super(outputStream, csvFormat, compression)
        }

        @Override
        protected Object getValue(Entry entry, String columnName) {
            Objects.requireNonNull(columnName, "columnName must not be null!")
            switch (columnName) {
                case "key":
                    return entry.key
                case "value":
                    return entry.value
                default:
                    throw new DrainException("Unknown column name '" + columnName + "'!")
            }
        }
    }
}
