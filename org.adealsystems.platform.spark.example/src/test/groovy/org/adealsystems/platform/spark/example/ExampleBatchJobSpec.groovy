/*
 * Copyright 2020 ADEAL Systems GmbH
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

package org.adealsystems.platform.spark.example

import static org.adealsystems.platform.DataInstances.createWriter
import static org.adealsystems.platform.spark.test.BatchTestTools.TODAY
import static org.adealsystems.platform.spark.test.BatchTestTools.readBatchLines

import org.adealsystems.platform.DataFormat
import org.adealsystems.platform.DataIdentifier
import org.adealsystems.platform.DataLocation
import org.adealsystems.platform.spark.test.spock.AbstractBatchJobSpec

class ExampleBatchJobSpec extends AbstractBatchJobSpec {
    private static final DataIdentifier INPUT_IDENTIFIER = new DataIdentifier("some_exporter", "input", DataFormat.CSV_SEMICOLON)

    private static final String[] INPUT_DATA = [
            'Date;COL1;COL2',
            '2020-05-14;Row1Col1;Row1Col2',
            '2020-01-31;Row2Col1;Row2Col2',
            '2019-11-15;Row3Col1;Row3Col2',
    ]

    private static final String[] OUTPUT_DATA = [
            'Date;WeekOfYear;COL1;COL2',
            '2019-11-15;2019W46;Row3Col1;Row3Col2',
            '2020-01-31;2020W05;Row2Col1;Row2Col2',
            '2020-05-14;2020W20;Row1Col1;Row1Col2',
    ]

    // this works, by naming convention, like @Before
    def setup() {
        def dataResolver = dataResolverRegistry.getResolverFor(DataLocation.INPUT)

        def currentInput = dataResolver.createCurrentInstance(INPUT_IDENTIFIER)
        createWriter(currentInput).withPrintWriter { writer ->
            INPUT_DATA.each { line ->
                writer.println line
            }
        }
        println "wrote inputData for " + currentInput
    }

    def "execute job"() {
        given:
        ExampleBatchJob instance = new ExampleBatchJob(dataResolverRegistry, TODAY)

        when:
        instance.init(sparkSession)
        instance.execute()
        instance.close()

        then:
        def outputIdentifier = instance.getOutputIdentifier()
        def dataResolver = dataResolverRegistry.getResolverFor(DataLocation.OUTPUT)
        def outputInstance = dataResolver.createCurrentInstance(outputIdentifier)
        def outputLines = readBatchLines(outputInstance)
        outputLines == OUTPUT_DATA
    }
}
