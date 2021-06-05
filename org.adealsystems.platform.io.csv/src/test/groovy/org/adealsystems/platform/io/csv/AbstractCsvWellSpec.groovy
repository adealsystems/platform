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
import org.adealsystems.platform.io.WellException
import org.adealsystems.platform.io.compression.Compression
import org.apache.commons.csv.CSVFormat
import spock.lang.Specification

class AbstractCsvWellSpec extends Specification {
    final CSVFormat OUTPUT_CSV_FORMAT = CSVFormat.DEFAULT
        .withHeader("key", "value")
        .withDelimiter(';' as char)

    final CSVFormat INPUT_CSV_FORMAT = CSVFormat.DEFAULT
        .withFirstRecordAsHeader()
        .withDelimiter(';' as char)

    def 'iterating over data with compression #compression works'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT, compression)
        instance.add(new Entry("Key 1", "Value 1"))
        instance.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        instance.close()

        when:
        EntryCsvWell well = new EntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT, compression)
        then:
        !well.isConsumed()

        when:
        List<Entry> objects = well.iterator().collect()

        then:
        objects == [
            new Entry("Key 1", "Value 1"),
            new Entry("Key 2", "Value 2"),
            new Entry("Key 3", "Value 3"),
        ]
        and:
        well.isConsumed()

        where:
        compression << [
            Compression.NONE,
            Compression.GZIP,
            Compression.BZIP,
        ]
    }

    def 'iterating over data with transposed columns works'() {
        given:
        String content = "value;key\n" +
            "Value 1;Key 1\n" +
            "Value 2;Key 2\n" +
            "Value 3;Key 3\n"
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        def writer = Compression.createWriter(bos, Compression.NONE)
        writer.write(content)
        writer.close()

        when:
        EntryCsvWell instance = new EntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT)
        then:
        !instance.isConsumed()

        when:
        List<Entry> objects = instance.iterator().collect()

        then:
        objects == [
            new Entry("Key 1", "Value 1"),
            new Entry("Key 2", "Value 2"),
            new Entry("Key 3", "Value 3"),
        ]
        and:
        instance.isConsumed()
    }

    def "calling iterator() twice throws exception"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        drain.add(new Entry("Key 1", "Value 1"))
        drain.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        drain.close()

        EntryCsvWell instance = new EntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT)

        when:
        instance.iterator()
        and:
        instance.iterator()

        then:
        WellException ex = thrown()
        ex.message == "A well can only be iterated once!"
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

    static class EntryCsvWell extends AbstractCsvWell<Entry> {

        EntryCsvWell(InputStream inputStream, CSVFormat csvFormat) throws IOException {
            super(Entry, inputStream, csvFormat)
        }

        EntryCsvWell(InputStream inputStream, CSVFormat csvFormat, Compression compression) throws IOException {
            super(Entry, inputStream, csvFormat, compression)
        }

        @Override
        protected void setValue(Entry entry, String columnName, String value) {
            Objects.requireNonNull(entry, "entry must not be null!")
            Objects.requireNonNull(columnName, "columnName must not be null!")
            switch (columnName) {
                case "key":
                    entry.setKey(value)
                    break
                case "value":
                    entry.setValue(value)
                    break
                default:
                    // ignore
                    // other implementations might want to be harsher
                    break
            }
        }
    }
}
