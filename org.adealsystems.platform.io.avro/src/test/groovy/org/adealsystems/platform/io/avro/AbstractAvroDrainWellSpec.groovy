/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

package org.adealsystems.platform.io.avro

import org.adealsystems.platform.io.DrainException
import org.adealsystems.platform.io.WellException
import org.adealsystems.platform.io.test.BrokenInputStream
import org.adealsystems.platform.io.test.BrokenOutputStream
import org.adealsystems.platform.io.test.BrokenStreamException
import org.apache.avro.file.CodecFactory
import spock.lang.Specification

class AbstractAvroDrainWellSpec extends Specification {
    def "writing to drain and then reading from well works with codec #codec"() {
        given:
        CodecFactory codecFactory = CodecFactory.fromString(codec)
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        EntryAvroDrain drain = new EntryAvroDrain(EntryAvroConstants.SCHEMA, codecFactory, bos)
        def entry1 = new Entry(1, "Value 1")
        def entry2 = new Entry(2, "Value 2")
        def entry3 = new Entry(3, "Value 3")

        when: 'entries are added'
        drain.add(entry1)
        drain.addAll([entry2, entry3])

        and: 'drain is closed'
        drain.close()
        and: 'we take those bytes and put them into a matching well'
        def bytes = bos.toByteArray()
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes)
        // correct codec is determined automatically from header
        EntryAvroWell well = new EntryAvroWell(EntryAvroConstants.SCHEMA, bis)
        and: 'we read all entries from the well'
        List<Entry> entries = well.collect()
        and: 'the well is closed'
        well.close()

        then: 'the entries read from the well are the same we put into the drain'
        entries != null
        entries.size() == 3
        entries[0] == entry1
        entries[1] == entry2
        entries[2] == entry3

        where:
        codec << ["null",
                  "deflate",
                  "snappy",
                  "bzip2",
                  "xz",
                  "zstandard"]
    }

    def 'add(..) throws exception if already closed'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractAvroDrain<Entry> instance = new EntryAvroDrain(EntryAvroConstants.SCHEMA, CodecFactory.nullCodec(), bos)

        when:
        instance.close()
        and:
        instance.add(new Entry(1, "Value 1"))

        then:
        DrainException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'add(..) throws exception if value is invalid'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractAvroDrain<Entry> instance = new EntryAvroDrain(EntryAvroConstants.SCHEMA, CodecFactory.nullCodec(), bos)

        when: 'we add an invalid value'
        instance.add(new Entry(1, null))

        then:
        DrainException ex = thrown()
        ex.message == "Exception while appending entry!"
        ex.cause.cause.class == NullPointerException
    }

    def 'drain.getSchema() returns given Schema'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractAvroDrain<Entry> instance = new EntryAvroDrain(EntryAvroConstants.SCHEMA, CodecFactory.nullCodec(), bos)

        expect:
        instance.getSchema() == EntryAvroConstants.SCHEMA
    }

    def 'closing drain twice is fine'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractAvroDrain<Entry> instance = new EntryAvroDrain(EntryAvroConstants.SCHEMA, CodecFactory.nullCodec(), bos)

        when: 'closing drain twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def 'well works as expected in edge cases'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractAvroDrain<Entry> drain = new EntryAvroDrain(EntryAvroConstants.SCHEMA, CodecFactory.nullCodec(), bos)
        def value1 = new Entry(1, "Value 1")
        drain.add(value1)
        drain.close()
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray())
        AbstractAvroWell<Entry> instance = new EntryAvroWell(EntryAvroConstants.SCHEMA, bis)

        expect:
        instance.getSchema() == EntryAvroConstants.SCHEMA
        !instance.isConsumed()

        when:
        def iterator = instance.iterator()
        then:
        instance.isConsumed()

        when: 'we try to obtain iterator from consumed well'
        instance.iterator()
        then:
        WellException ex = thrown()
        ex.message == "A well can only be iterated once!"

        when:
        def value = iterator.next()
        then:
        value == value1

        when:
        iterator.next()
        then:
        thrown(NoSuchElementException)

        when: 'closing drain twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def 'iterating a closed well fails as expected'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractAvroDrain<Entry> drain = new EntryAvroDrain(EntryAvroConstants.SCHEMA, CodecFactory.nullCodec(), bos)
        def value1 = new Entry(1, "Value 1")
        def value2 = new Entry(2, "Value 2")
        drain.addAll([value1, value2])
        drain.close()
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray())
        AbstractAvroWell<Entry> instance = new EntryAvroWell(EntryAvroConstants.SCHEMA, bis)
        def iterator = instance.iterator()

        expect:
        iterator.next() == value1

        when:
        instance.close()
        iterator.next()

        then:
        WellException ex = thrown()
        ex.message == "Well was already closed!"
    }

    def 'expected exception is thrown if reading invalid data'() {
        given: 'data written with relaxed schema allowing null Entry.value'
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractAvroDrain<Entry> drain = new EntryAvroDrain(EntryAvroConstants.RELAXED_SCHEMA, CodecFactory.nullCodec(), bos)
        def value1 = new Entry(1, "Value 1")
        def value2 = new Entry(2, null)

        drain.addAll([value1, value2])
        drain.close()
        def bytes = bos.toByteArray()
        and: 'a well with strict schema not allowing null Entry.value'
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes)
        AbstractAvroWell<Entry> instance = new EntryAvroWell(EntryAvroConstants.SCHEMA, bis)
        def iterator = instance.iterator()

        expect: 'first value can be read since it has an Entry.value'
        iterator.next() == value1

        when: 'trying to read second value with null Entry.value'
        iterator.next()

        then: 'expected exception is thrown'
        WellException ex = thrown()
        ex.message == "Exception while reading record!"
        ex.cause != null
    }

    def "exception while closing drain is handled as expected"() {
        given:
        BrokenOutputStream bos = new BrokenOutputStream(new ByteArrayOutputStream())
        AbstractAvroDrain<Entry> drain = new EntryAvroDrain(EntryAvroConstants.SCHEMA, CodecFactory.nullCodec(), bos)
        def value1 = new Entry(1, "Value 1")
        drain.add(value1)
        bos.broken = true

        when:
        drain.close()

        then:
        DrainException ex = thrown()
        ex.message == "Exception while closing dataFileWriter!"
        ex.cause instanceof BrokenStreamException
    }

    def "exception while closing well is handled as expected"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        AbstractAvroDrain<Entry> drain = new EntryAvroDrain(EntryAvroConstants.SCHEMA, CodecFactory.nullCodec(), bos)
        def value1 = new Entry(1, "Value 1")
        drain.add(value1)
        drain.close()
        def bytes = bos.toByteArray()
        BrokenInputStream bis = new BrokenInputStream(new ByteArrayInputStream(bytes))
        AbstractAvroWell<Entry> well = new EntryAvroWell(EntryAvroConstants.SCHEMA, bis)
        bis.broken = true

        when:
        well.close()

        then:
        WellException ex = thrown()
        ex.message == "Exception while closing well!"
        ex.cause instanceof BrokenStreamException
    }
}
