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

import org.adealsystems.platform.io.WellException
import org.adealsystems.platform.io.compression.Compression
import spock.lang.Specification
import spock.lang.Unroll

class JsonlWellSpec extends Specification {
    @Unroll
    def 'iterating over data with compression #compression works'() {
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
        compression << [
                Compression.NONE,
                Compression.GZIP,
                Compression.BZIP,
        ]
    }

    def "calling iterator() twice throws exception"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        JsonlDrain<Entry> drain = new JsonlDrain<>(bos)
        drain.add(new Entry("Value 1"))
        drain.addAll([new Entry("Value 2"), new Entry("Value 3")])
        drain.close()

        JsonlWell<Entry> instance = new JsonlWell<>(Entry, new ByteArrayInputStream(bos.toByteArray()))

        when:
        instance.iterator()
        and:
        instance.iterator()

        then:
        WellException ex = thrown()
        ex.message == "A well can only be iterated once!"
    }

    private static class Entry {
        String value

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
}
