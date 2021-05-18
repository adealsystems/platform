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

package org.adealsystems.platform.file

import org.adealsystems.platform.DataIdentifier
import org.adealsystems.platform.DataInstance
import org.adealsystems.platform.DataResolver
import org.adealsystems.platform.DefaultNamingStrategy
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Shared
import spock.lang.Specification

import static org.adealsystems.platform.DataFormat.CSV_SEMICOLON

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path

class FileDataResolutionStrategySpec extends Specification {
    private static Logger LOGGER = LoggerFactory.getLogger(FileDataResolutionStrategySpec)


    @Shared
    private Set<Path> tempCollector = new HashSet<>()

    def 'trying to read missing data causes exception'() {
        given:
        Path tempDirectory = Files.createTempDirectory("tempDir")
        tempCollector.add(tempDirectory)

        DataResolver dataResolver = createDataResolver(tempDirectory)

        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)

        when:
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        and:
        dataInstance.inputStream

        then:
        NoSuchFileException ex = thrown()
        ex.message.endsWith("source/current/use_case/source_use_case.csv")
    }

    def 'writing and reading data works'() {
        given:
        Path tempDirectory = Files.createTempDirectory("tempDir")
        tempCollector.add(tempDirectory)
        DataResolver dataResolver = createDataResolver(tempDirectory)
        DataIdentifier dataIdentifier = new DataIdentifier("source", "use_case", CSV_SEMICOLON)

        when:
        DataInstance dataInstance = dataResolver.createCurrentInstance(dataIdentifier)
        and:
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(dataInstance.outputStream))
        out.println("Foo")
        out.close()
        and:
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInstance.inputStream))
        String line = reader.readLine()

        then:
        line == "Foo"
    }

    def 'creating instance with file causes exception'() {
        given:
        Path tempDirectory = Files.createTempFile("tempFile", "")
        tempCollector.add(tempDirectory)

        when:
        createDataResolver(tempDirectory)

        then:
        IllegalArgumentException ex = thrown()
        ex.message.endsWith("is not a directory!")
    }

    def 'creating instance with non-existent file creates directory'() {
        given:
        Path tempDirectory = Files.createTempFile("tempFile", "")
        tempCollector.add(tempDirectory)
        Files.delete(tempDirectory)

        when:
        createDataResolver(tempDirectory)

        then:
        Files.isDirectory(tempDirectory)
    }

    def cleanupSpec() {
        tempCollector.each { it ->
            if (Files.isDirectory(it)) {
                if (it.deleteDir()) {
                    LOGGER.debug("Deleted directory {}.", it)
                }
            } else {
                if (Files.deleteIfExists(it)) {
                    LOGGER.debug("Deleted file {}.", it)
                }
            }
        }
    }

    private static DataResolver createDataResolver(Path basePath) {
        return new DataResolver(new FileDataResolutionStrategy(new DefaultNamingStrategy(), basePath.toFile()))
    }
}
