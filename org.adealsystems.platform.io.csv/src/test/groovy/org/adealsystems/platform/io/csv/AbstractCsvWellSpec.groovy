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
        def writer = Compression.NONE.createWriter(bos)
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

    def "getHeaders() returns expected value"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        drain.add(new Entry("Key 1", "Value 1"))
        drain.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        drain.close()

        EntryCsvWell instance = new EntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT)

        expect:
        instance.headers == ['key', 'value']
    }

    def "getHeaders() is unmodifiable"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        drain.add(new Entry("Key 1", "Value 1"))
        drain.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        drain.close()

        EntryCsvWell instance = new EntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT)

        when:
        instance.headers.remove('key')

        then:
        thrown(UnsupportedOperationException)
    }

    def "closing twice is ok"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        drain.add(new Entry("Key 1", "Value 1"))
        drain.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        drain.close()

        EntryCsvWell instance = new EntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT)

        when: 'well is closed twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def "class without default constructor causes expected exception"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        drain.add(new Entry("Key 1", "Value 1"))
        drain.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        drain.close()

        BrokenEntryCsvWell instance = new BrokenEntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT)

        when:
        def iterator = instance.iterator()
        iterator.next()

        then:
        WellException ex = thrown()
        ex.printStackTrace()
        ex.message == "Exception while creating instance of class org.adealsystems.platform.io.csv.BrokenEntry!"
    }

    def "reading from closed drain causes expected exception"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        drain.add(new Entry("Key 1", "Value 1"))
        drain.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        drain.close()

        EntryCsvWell instance = new EntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT)
        def iterator = instance.iterator()

        when:
        def read = iterator.next()
        then:
        read == new Entry("Key 1", "Value 1")

        when:
        instance.close()
        iterator.next()

        then:
        WellException ex = thrown()
        ex.message == "Well was already closed!"
    }

    def "iterating too much causes expected exception"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        drain.add(new Entry("Key 1", "Value 1"))
        drain.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        drain.close()

        EntryCsvWell instance = new EntryCsvWell(new ByteArrayInputStream(bos.toByteArray()), INPUT_CSV_FORMAT)
        def iterator = instance.iterator()

        when:
        iterator.next()
        iterator.next()
        iterator.next()
        iterator.next()

        then:
        thrown(NoSuchElementException)
    }

    def "exception while reading is handled as expected"() {
        given: 'more data than the internal buffer size is available'
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        final int count = 8000
        for (int i = 0; i < count; i++) {
            drain.add(new Entry("Key " + i, "Value " + i))
        }
        drain.close()

        BrokenInputStream brokenStream = new BrokenInputStream(new ByteArrayInputStream(bos.toByteArray()))
        EntryCsvWell instance = new EntryCsvWell(brokenStream, INPUT_CSV_FORMAT)
        def iterator = instance.iterator()
        when: 'internal buffer is exhausted with stream set to broken'
        brokenStream.setBroken(true)
        for (int i = 0; i < count; i++) {
            iterator.next()
        }

        then: 'at some point the expected exception is actually thrown'
        WellException ex = thrown()
        ex.message == "Exception while reading record!"
        ex.cause.message.contains("nope")
    }

    def "exception while closing is handled as expected"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> drain = new EntryCsvDrain(bos, OUTPUT_CSV_FORMAT)
        drain.add(new Entry("Key 1", "Value 1"))
        drain.addAll([new Entry("Key 2", "Value 2"), new Entry("Key 3", "Value 3")])
        drain.close()

        BrokenInputStream brokenStream = new BrokenInputStream(new ByteArrayInputStream(bos.toByteArray()))
        EntryCsvWell instance = new EntryCsvWell(brokenStream, INPUT_CSV_FORMAT)
        brokenStream.setBroken(true)

        when:
        instance.close()

        then:
        WellException ex = thrown()
        ex.message == "Exception while closing parser!"
        ex.cause.message.startsWith("nope")
    }
}
