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

package org.adealsystems.platform.id.hadoop

import org.adealsystems.platform.id.DataFormat
import org.adealsystems.platform.id.DataIdentifier
import org.adealsystems.platform.id.DataInstance
import org.adealsystems.platform.id.DataResolutionCapability
import org.adealsystems.platform.id.DataResolver
import org.adealsystems.platform.id.DefaultNamingStrategy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.charset.StandardCharsets

class HadoopDataResolutionStrategySpec extends Specification {
    @TempDir
    File temp

    def "written data can be read again"() {
        given:
        HadoopDataResolutionStrategy instance = createInstance(temp)
        DataResolver dataResolver = new DataResolver(instance)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)

        when:
        writeData(dataInstance.outputStream)
        and:
        String read = readData(dataInstance.inputStream)

        then:
        read == CONTENT
    }

    def "written data can't be read after delete"() {
        given:
        HadoopDataResolutionStrategy instance = createInstance(temp)
        DataResolver dataResolver = new DataResolver(instance)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)

        when:
        writeData(dataInstance.outputStream)
        and:
        boolean deleted = dataInstance.delete()
        and:
        readData(dataInstance.inputStream)

        then:
        deleted
        thrown(FileNotFoundException)
    }

    def "path works as expected"() {
        given:
        String tempUri = temp.toURI().toURL().toString()
        HadoopDataResolutionStrategy instance = createInstance(temp)
        DataResolver dataResolver = new DataResolver(instance)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", DataFormat.CSV_SEMICOLON)
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)

        when:
        def path = dataInstance.path

        then:
        path.startsWith(tempUri)
        path.contains("source")
        path.contains("use_case")
        path.contains(".csv")
    }

    def "instance has expected capabilities"() {
        given:
        HadoopDataResolutionStrategy instance = createInstance(temp)

        expect:
        instance.capabilities == DataResolutionCapability.ALL
    }

    def "instance supports expected capability #capability"() {
        given:
        HadoopDataResolutionStrategy instance = createInstance(temp)

        expect:
        instance.supports(capability)

        where:
        capability << DataResolutionCapability.ALL
    }

    def "null values throw expected exceptions"() {
        given:
        HadoopDataResolutionStrategy instance = createInstance(temp)

        when:
        instance.getInputStream(null)
        then:
        NullPointerException ex = thrown()
        ex.message == "dataInstance must not be null!"

        when:
        instance.getOutputStream(null)
        then:
        ex = thrown()
        ex.message == "dataInstance must not be null!"

        when:
        instance.delete(null)
        then:
        ex = thrown()
        ex.message == "dataInstance must not be null!"

        when:
        instance.getPath(null)
        then:
        ex = thrown()
        ex.message == "dataInstance must not be null!"

        when:
        instance.supports(null)
        then:
        ex = thrown()
        ex.message == "capability must not be null!"
    }

    private static final String CONTENT = readData(HadoopDataResolutionStrategySpec.class.getResourceAsStream("/content.txt"))

    private static HadoopDataResolutionStrategy createInstance(File temp) {
        FileSystem fileSystem = FileSystem.getLocal(new Configuration())
        return new HadoopDataResolutionStrategy(fileSystem, new DefaultNamingStrategy(), temp.toURI().toURL().toString())
    }

    private static void writeData(OutputStream os) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))) {
            writer.write(CONTENT)
        }
    }

    private static String readData(InputStream is) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder builder = new StringBuilder()
            boolean first = true
            for (String line : reader.readLines()) {
                if (line == null) {
                    continue
                }
                if (first) {
                    first = false
                } else {
                    builder.append('\n')
                }
                builder.append(line)
            }
            return builder.toString()
        }
    }
}
