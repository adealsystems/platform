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

package org.adealsystems.platform.io.compression

import spock.lang.Specification
import spock.lang.Unroll

class CompressionSpec extends Specification {

    @Unroll
    def "compression and decompression works for #compression"() {
        given:
        ByteArrayOutputStream bos = new ByteArrayOutputStream()

        when:
        def writer = Compression.createWriter(bos, compression)
        for (line in CONTENT) {
            writer.write(line)
            writer.write('\n' as char)
        }
        writer.flush()
        writer.close()

        and:
        def reader = Compression.createReader(new ByteArrayInputStream(bos.toByteArray()), compression)
        def lines = reader.readLines()

        then:
        lines == CONTENT

        where:
        compression << [
                Compression.NONE,
                Compression.GZIP,
                Compression.BZIP,
        ]
    }

    private static final List<String> CONTENT = [
            "Some content",
            "Some content",
            "Some content",
            "Some content",
            "Some content",
            "Some content",
            "Some content",
            "Some content",
    ]
}
