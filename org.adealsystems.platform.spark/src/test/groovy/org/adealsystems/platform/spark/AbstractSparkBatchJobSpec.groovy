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

package org.adealsystems.platform.spark

import org.adealsystems.platform.DataFormat
import org.adealsystems.platform.DataIdentifier
import org.adealsystems.platform.DataLocation
import org.adealsystems.platform.DataResolverRegistry
import org.adealsystems.platform.spark.test.BatchTestTools
import org.adealsystems.platform.spark.test.spock.AbstractBatchJobSpec
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.UDFRegistration

import java.time.LocalDate

class AbstractSparkBatchJobSpec extends AbstractBatchJobSpec {

    private static final LocalDate SOME_DATE = LocalDate.of(2020, 4, 1)

    def 'inputs are registered as expected'() {
        given:
        TestBatchJob instance = new TestBatchJob(dataResolverRegistry)

        when:
        instance.init(sparkSession)

        then:
        def inputs = instance.getInputInstances()
        def currentInput = inputs[TestBatchJob.CURRENT_INPUT_IDENTIFIER]
        def todayInput = inputs[TestBatchJob.TODAY_INPUT_IDENTIFIER]
        def someDateInput = inputs[TestBatchJob.SOME_DATE_INPUT_IDENTIFIER]

        currentInput != null
        todayInput != null
        someDateInput != null

        and:
        currentInput.size() == 1
        todayInput.size() == 1
        someDateInput.size() == 1

        and:
        def currentDate = currentInput.stream().findFirst().get().getDate()
        def todayDate = todayInput.stream().findFirst().get().getDate()
        def someDate = someDateInput.stream().findFirst().get().getDate()

        !currentDate.isPresent()
        todayDate.isPresent()
        someDate.isPresent()

        and:
        todayDate.get() == BatchTestTools.TODAY
        someDate.get() == SOME_DATE
    }

    static class TestBatchJob extends AbstractSparkBatchJob {
        static final DataIdentifier CURRENT_INPUT_IDENTIFIER = new DataIdentifier("input", "without_date", DataFormat.CSV_SEMICOLON)
        static final DataIdentifier TODAY_INPUT_IDENTIFIER = new DataIdentifier("input", "with_today_date", DataFormat.CSV_SEMICOLON)
        static final DataIdentifier SOME_DATE_INPUT_IDENTIFIER = new DataIdentifier("input", "with_some_date", DataFormat.CSV_SEMICOLON)


        TestBatchJob(DataResolverRegistry dataResolverRegistry) {
            super(dataResolverRegistry, DataLocation.OUTPUT, new DataIdentifier("output_source", "output_use_case", DataFormat.CSV_SEMICOLON), BatchTestTools.TODAY)
        }

        @Override
        protected void registerUdfs(UDFRegistration udfRegistration) {

        }

        @Override
        protected void registerInputs() {
            registerCurrentInput(DataLocation.INPUT, CURRENT_INPUT_IDENTIFIER)
            registerInput(DataLocation.INPUT, TODAY_INPUT_IDENTIFIER, getInvocationDate())
            registerInput(DataLocation.INPUT, SOME_DATE_INPUT_IDENTIFIER, SOME_DATE)
        }

        @Override
        protected Dataset<Row> processData() {
            return null
        }
    }
}
