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

package org.adealsystems.platform.io.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.adealsystems.platform.io.compression.Compression
import spock.lang.Specification

class JsonlDrainSpec extends Specification {

    def 'adding to the drain with compression #compression works'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        JsonlDrain<Entry> instance = new JsonlDrain<>(bos, compression)

        when:
        instance.add(new Entry("Entry 1"))
        and:
        instance.addAll([new Entry("Entry 2"), new Entry("Entry 3")])
        and:
        instance.close()

        and:
        List<String> lines = readLines(bos.toByteArray(), compression)

        then:
        lines == ['{"value":"Entry 1"}', '{"value":"Entry 2"}', '{"value":"Entry 3"}']

        when:
        List<Entry> objects = parseLines(lines)

        then:
        objects == [
            new Entry("Entry 1"),
            new Entry("Entry 2"),
            new Entry("Entry 3"),
        ]

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
        JsonlDrain<Entry> instance = new JsonlDrain<>(bos)

        when:
        instance.close()
        and:
        instance.add(new Entry("Entry 1"))

        then:
        IllegalStateException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'addAll(..) throws exception if already closed'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        JsonlDrain<Entry> instance = new JsonlDrain<>(bos)

        when:
        instance.close()
        and:
        instance.addAll([new Entry("Entry 2"), new Entry("Entry 3")])

        then:
        IllegalStateException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'add(null) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        JsonlDrain<Entry> instance = new JsonlDrain<>(bos)

        when:
        instance.add(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entry must not be null!"
    }

    def 'addAll(null) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        JsonlDrain<Entry> instance = new JsonlDrain<>(bos)

        when:
        instance.addAll(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not be null!"
    }

    def 'addAll([null]) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        JsonlDrain<Entry> instance = new JsonlDrain<>(bos)

        when:
        instance.addAll([null])

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not contain null!"
    }

    def 'creating instance with indenting ObjectMapper fails'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        ObjectMapper objectMapper = new ObjectMapper()
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT)

        when:
        new JsonlDrain<>(bos, objectMapper)

        then:
        IllegalArgumentException ex = thrown()
        ex.message == "objectMapper must not have INDENT_OUTPUT feature enabled!"
    }

    private static class Entry {
        String value

        @SuppressWarnings('unused')
        Entry() {
        }

        Entry(String value) {
            this.value = value
        }

        boolean equals(o) {
            if (this.is(o)) return true
            if (getClass() != o.class) return false

            Entry entry = (Entry) o

            if (value != entry.value) return false

            return true
        }

        int hashCode() {
            return (value != null ? value.hashCode() : 0)
        }

        @Override
        String toString() {
            return "Entry{" +
                "value='" + value + '\'' +
                '}'
        }
    }

    private static List<String> readLines(byte[] bytes, Compression compression) {
        Objects.requireNonNull(compression, "compression must not be null!")
        return Compression.createReader(new ByteArrayInputStream(bytes), compression).readLines()
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()

    private static List<Entry> parseLines(List<String> lines) {
        List<Entry> result = new ArrayList<>()
        for (String line : lines) {
            result.add(OBJECT_MAPPER.readValue(line, Entry.class))
        }
        return result
    }
}
