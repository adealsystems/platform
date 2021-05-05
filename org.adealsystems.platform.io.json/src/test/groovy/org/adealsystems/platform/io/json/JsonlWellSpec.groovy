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

import org.adealsystems.platform.io.compression.Compression
import spock.lang.Specification
import spock.lang.Unroll

class JsonlWellSpec extends Specification {
    @Unroll
    def 'iterating over data with compression #compression works'() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        JsonlDrain<Entry> instance = new JsonlDrain<>(bos, compression)
        instance.add(new Entry("Entry 1"))
        instance.addAll([new Entry("Entry 2"), new Entry("Entry 3")])
        instance.close()

        when:
        JsonlWell<Entry> well = new JsonlWell<>(Entry, new ByteArrayInputStream(bos.toByteArray()), compression)
        then:
        !well.isConsumed()

        when:
        List<Entry> objects = well.iterator().collect()

        then:
        objects == [
                new Entry("Entry 1"),
                new Entry("Entry 2"),
                new Entry("Entry 3"),
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
