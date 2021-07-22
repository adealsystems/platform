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
import org.adealsystems.platform.io.ListWell
import org.adealsystems.platform.io.Well
import org.adealsystems.platform.io.WellException
import org.adealsystems.platform.io.compression.Compression
import spock.lang.Specification

import java.nio.charset.StandardCharsets

class JsonlWellSpec extends Specification {

    def 'iterating over data with compression #compression works'(Compression compression) {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        JsonlDrain<Entry> drain = new JsonlDrain<>(bos, compression)
        drain.add(new Entry("Entry 1"))
        drain.addAll([new Entry("Entry 2"), new Entry("Entry 3")])
        drain.close()

        when:
        JsonlWell<Entry> instance = new JsonlWell<>(Entry, new ByteArrayInputStream(bos.toByteArray()), compression)
        then:
        !instance.isConsumed()

        when:
        List<Entry> objects = instance.iterator().collect()

        then:
        objects == [
            new Entry("Entry 1"),
            new Entry("Entry 2"),
            new Entry("Entry 3"),
        ]
        and:
        instance.isConsumed()

        where:
        compression << Compression.values()
    }

    def 'this constructor also works'() {
        when:
        JsonlWell<Entry> instance = new JsonlWell<>(Entry, new ByteArrayInputStream(getExampleBytes()))
        then:
        !instance.isConsumed()

        when:
        List<Entry> objects = instance.iterator().collect()

        then:
        objects == [
            new Entry("Entry 1"),
            new Entry("Entry 2"),
            new Entry("Entry 3"),
        ] as List
        and:
        instance.isConsumed()
    }

    def 'and even this constructor works'() {
        when:
        JsonlWell<Entry> instance = new JsonlWell<>(Entry, new ByteArrayInputStream(getExampleBytes()), new ObjectMapper())
        then:
        !instance.isConsumed()

        when:
        List<Entry> objects = instance.iterator().collect()

        then:
        objects == [
            new Entry("Entry 1"),
            new Entry("Entry 2"),
            new Entry("Entry 3"),
        ]
        and:
        instance.isConsumed()
    }

    def "calling iterator() twice throws exception"() {
        given:
        JsonlWell<Entry> instance = new JsonlWell<>(Entry, new ListWell<String>())

        when:
        instance.iterator()
        and:
        instance.iterator()

        then:
        WellException ex = thrown()
        ex.message == "A well can only be iterated once!"
    }

    def "closing causes expected exception while iterating"() {
        given:
        Well<String> stringWell = new ListWell<String>(["{\"value\":\"Entry 1\"}", "{\"value\":\"Entry 2\"}", "{\"value\":\"Entry 3\"}"])
        JsonlWell instance = new JsonlWell(Entry, stringWell)

        def iterator = instance.iterator()

        expect: 'an entry can be read'
        new Entry("Entry 1") == iterator.next()
        when: 'well is closed'
        instance.close()
        and: 'next value is requested'
        iterator.next()
        then: 'the expected exception is thrown'
        WellException ex = thrown()
        ex.message == "Well was already closed!"
    }

    def "closing twice is ok"() {
        given:
        JsonlWell instance = new JsonlWell(Entry, new ListWell<String>(new ArrayList<String>()))

        when: 'well is closed twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def "broken json causes expected exception while iterating"() {
        given:
        Well<String> stringWell = new ListWell<String>(["{\"value\":\"Entry 1\"}", "Nope", "{\"value\":\"Entry 3\"}"])
        JsonlWell instance = new JsonlWell(Entry, stringWell)

        def iterator = instance.iterator()

        expect: 'an entry can be read'
        new Entry("Entry 1") == iterator.next()

        when: 'next broken value is requested'
        iterator.next()
        then: 'the expected exception is thrown'
        WellException ex = thrown()
        ex.message == "Failed to parse JSON!"
    }

    def "exception while closing is handled as expected"() {
        given:
        Well<String> badWell = Mock()
        JsonlWell instance = new JsonlWell(Entry, badWell)

        when:
        instance.close()

        then:
        badWell.close() >> { throw new IllegalStateException("nope") }
        WellException ex = thrown()
        ex.message == "Exception while closing well!"
        ex.cause.message.startsWith("nope")
    }

    private static byte[] getExampleBytes() {
        "{\"value\":\"Entry 1\"}\n{\"value\":\"Entry 2\"}\n{\"value\":\"Entry 3\"}\n".getBytes(StandardCharsets.UTF_8)
    }
}
