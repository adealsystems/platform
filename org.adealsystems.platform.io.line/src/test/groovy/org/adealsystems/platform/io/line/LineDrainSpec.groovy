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

import org.adealsystems.platform.io.DrainException
import org.adealsystems.platform.io.compression.Compression
import spock.lang.Specification

class LineDrainSpec extends Specification {

    def 'adding to the drain with compression #compression works'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain instance = new LineDrain(bos, compression)

        when:
        instance.add("Line 1")
        and:
        instance.addAll(["Line 2", "Line 3"])
        and:
        instance.close()

        and:
        List<String> lines = readLines(bos.toByteArray(), compression)

        then:
        lines == ["Line 1", "Line 2", "Line 3"]

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
        LineDrain instance = new LineDrain(bos)

        when:
        instance.close()
        and:
        instance.add("Line 1")

        then:
        DrainException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'addAll(..) throws exception if already closed'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain instance = new LineDrain(bos)

        when:
        instance.close()
        and:
        instance.addAll(["Line 2", "Line 3"])

        then:
        DrainException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'add(null) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain instance = new LineDrain(bos)

        when:
        instance.add(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entry must not be null!"
    }

    def 'addAll(null) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain instance = new LineDrain(bos)

        when:
        instance.addAll(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not be null!"
    }

    def 'addAll([null]) throws exception'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain instance = new LineDrain(bos)

        when:
        instance.addAll([null])

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not contain null!"
    }

    def "closing twice is ok"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        LineDrain instance = new LineDrain(bos)

        when: 'well is closed twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def "exception while closing is handled as expected"() {
        given:
        BufferedWriter badWriter = Mock()
        LineDrain instance = new LineDrain(badWriter)

        when:
        instance.close()

        then:
        badWriter.close() >> { throw new IOException("nope") }
        DrainException ex = thrown()
        ex.message == "Exception while closing stream!"
        ex.cause.message.startsWith("nope")
    }

    def "exception while writing line is handled as expected"() {
        given:
        BufferedWriter badWriter = Mock()
        LineDrain instance = new LineDrain(badWriter)
        when:
        instance.add("line")
        then:
        2 * badWriter.write(_)

        when:
        instance.add("line")
        then:
        1 * badWriter.write(_) >> { throw new IOException("nope") }

        DrainException ex = thrown()
        ex.message == "Exception while writing line!"
        ex.cause.message.startsWith("nope")
    }

    private static List<String> readLines(byte[] bytes, Compression compression) {
        Objects.requireNonNull(compression, "compression must not be null!")
        return compression.createReader(new ByteArrayInputStream(bytes)).readLines()
    }
}
