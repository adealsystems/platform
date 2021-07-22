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

package org.adealsystems.platform.io.line

import org.adealsystems.platform.io.WellException
import org.adealsystems.platform.io.compression.Compression
import spock.lang.Specification

class LineWellSpec extends Specification {

    def 'iterating over data with compression #compression works'(Compression compression) {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain drain = new LineDrain(bos, compression)
        drain.add("Line 1")
        drain.addAll(["Line 2", "Line 3"])
        drain.close()

        when:
        LineWell instance = new LineWell(new ByteArrayInputStream(bos.toByteArray()), compression)
        then:
        !instance.isConsumed()

        when:
        List<String> values = instance.iterator().collect()

        then:
        values == ["Line 1", "Line 2", "Line 3"]
        and:
        instance.isConsumed()

        where:
        compression << Compression.values()
    }

    def "calling iterator() twice throws exception"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain drain = new LineDrain(bos)
        drain.add("Line 1")
        drain.addAll(["Line 2", "Line 3"])
        drain.close()

        LineWell instance = new LineWell(new ByteArrayInputStream(bos.toByteArray()))

        when:
        instance.iterator()
        and:
        instance.iterator()

        then:
        WellException ex = thrown()
        ex.message == "A well can only be iterated once!"
    }

    def "empty iterator behaves as expected"() {
        given: 'a well without data'
        LineWell instance = new LineWell(new ByteArrayInputStream(new byte[]{}))

        when:
        def iterator = instance.iterator()

        then:
        !iterator.hasNext()

        when:
        iterator.next()
        then:
        thrown(NoSuchElementException)
    }

    def "closing causes expected exception while iterating"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain drain = new LineDrain(bos)
        drain.addAll(["Line 1", "Line 2", "Line 3"])
        drain.close()

        LineWell instance = new LineWell(new ByteArrayInputStream(bos.toByteArray()))
        def iterator = instance.iterator()

        expect: 'a line can be read'
        "Line 1" == iterator.next()
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
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain drain = new LineDrain(bos)
        drain.addAll(["Line 1", "Line 2", "Line 3"])
        drain.close()

        LineWell instance = new LineWell(new ByteArrayInputStream(bos.toByteArray()))
        def iterator = instance.iterator()

        expect: 'a line can be read'
        "Line 1" == iterator.next()
        when: 'well is closed twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def "exception while closing is handled as expected"() {
        given:
        BufferedReader badReader = Mock()
        LineWell instance = new LineWell(badReader)

        when:
        instance.close()

        then:
        badReader.readLine() >> "line"
        badReader.close() >> { throw new IOException("nope") }
        WellException ex = thrown()
        ex.message == "Exception while closing stream!"
        ex.cause.message.startsWith("nope")
    }

    def "exception while reading line is handled as expected"() {
        given:
        BufferedReader badReader = Mock()
        LineWell instance = new LineWell(badReader)
        when:
        def iterator = instance.iterator()
        def result = iterator.next()
        then:
        2 * badReader.readLine() >> "line"
        result == "line"
        when:
        iterator.next()
        then:
        1 * badReader.readLine() >> { throw new IOException("nope") }
        WellException ex = thrown()
        ex.message == "Exception while reading line!"
        ex.cause.message.startsWith("nope")
    }
}
