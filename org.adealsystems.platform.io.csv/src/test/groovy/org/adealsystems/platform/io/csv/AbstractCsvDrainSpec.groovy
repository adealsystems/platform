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
    final CSVFormat CSV_FORMAT = CSVFormat.Builder.create()
        .setHeader("key", "value")
        .setDelimiter(CsvConstants.CSV_DELIMITER_SEMICOLON)
        .setEscape(CsvConstants.CSV_ESCAPE_CHARACTER)
        .build()

    def 'adding to the drain with compression #compression works'(Compression compression) {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT, compression)

        expect:
        instance.CSVFormat == CSV_FORMAT

        when:
        instance.add(new Entry("Key 1", "Value 1"))
        and:
        instance.addAll([new Entry("Key 2", "Value \"2"), new Entry("Key 3", "Value 3")])
        and:
        instance.close()

        and:
        List<String> lines = readLines(bos.toByteArray(), compression)

        then:
        lines == ['key;value', 'Key 1;Value 1', 'Key 2;"Value \\"2"', 'Key 3;Value 3']

        where:
        compression << Compression.values()
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
        DrainException ex = thrown()
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
        DrainException ex = thrown()
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

    def "getHeaders() returns expected value"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT)

        expect:
        instance.headers == ['key', 'value']
    }

    def "getHeaders() is unmodifiable"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT)

        when:
        instance.headers.remove('key')

        then:
        thrown(UnsupportedOperationException)
    }

    def "closing twice is ok"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(bos, CSV_FORMAT)

        when: 'drain is closed twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def "exception while performing I/O is handled as expected for add"() {
        given:
        BufferedWriter badWriter = Mock()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(badWriter, CSV_FORMAT)

        when:
        instance.add(new Entry("Key 1", "Value 1"))

        then:
        badWriter.append(_ as CharSequence) >> { throw new IOException("nope") }
        DrainException ex = thrown()
        ex.message == "Exception while printing record!"
        ex.cause.message.startsWith("nope")
    }

    def "exception while performing I/O is handled as expected for addAll"() {
        given:
        BufferedWriter badWriter = Mock()
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(badWriter, CSV_FORMAT)

        when:
        instance.addAll([new Entry("Key 1", "Value 1")])

        then:
        badWriter.append(_ as CharSequence) >> { throw new IOException("nope") }
        DrainException ex = thrown()
        ex.message == "Exception while printing record!"
        ex.cause.message.startsWith("nope")
    }

    def "exception while closing is handled as expected"() {
        given:
        BufferedWriter badWriter = Mock()

        when:
        AbstractCsvDrain<Entry> instance = new EntryCsvDrain(badWriter, CSV_FORMAT)
        instance.close()

        then:
        badWriter.close() >> { throw new IllegalStateException("nope") }
        DrainException ex = thrown()
        ex.message == "Exception while closing printer!"
        ex.cause.message.startsWith("nope")
    }

    private static List<String> readLines(byte[] bytes, Compression compression) {
        Objects.requireNonNull(compression, "compression must not be null!")
        return compression.createReader(new ByteArrayInputStream(bytes)).readLines()
    }
}
