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

package org.adealsystems.platform.spark.example;

import org.adealsystems.platform.DataFormat;
import org.adealsystems.platform.DataIdentifier;
import org.adealsystems.platform.DataInstance;
import org.adealsystems.platform.DataLocation;
import org.adealsystems.platform.DataResolver;
import org.adealsystems.platform.spark.test.junit4.AbstractBatchJobTest;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;

import static org.adealsystems.platform.DataInstances.createWriter;
import static org.adealsystems.platform.spark.test.BatchTestTools.TODAY;
import static org.adealsystems.platform.spark.test.BatchTestTools.readBatchLines;
import static org.junit.Assert.assertArrayEquals;

public class ExampleBatchJobTest extends AbstractBatchJobTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExampleBatchJob.class);

    private static final DataIdentifier INPUT_IDENTIFIER = new DataIdentifier("some_exporter", "input", DataFormat.CSV_SEMICOLON);

    private static final String[] INPUT_DATA = {
            "Date;COL1;COL2",
            "2020-05-14;Row1Col1;Row1Col2",
            "2020-01-31;Row2Col1;Row2Col2",
            "2019-11-15;Row3Col1;Row3Col2",
    };

    private static final String[] OUTPUT_DATA = {
            "Date;WeekOfYear;COL1;COL2",
            "2019-11-15;2019W46;Row3Col1;Row3Col2",
            "2020-01-31;2020W05;Row2Col1;Row2Col2",
            "2020-05-14;2020W20;Row1Col1;Row1Col2",
    };

    @Before
    public void setUp() throws IOException {
        DataResolver dataResolver = dataResolverRegistry.getResolverFor(DataLocation.INPUT);
        DataInstance currentInput = dataResolver.createCurrentInstance(INPUT_IDENTIFIER);
        try (PrintWriter pw = createWriter(currentInput)) {
            for (String line : INPUT_DATA) {
                pw.println(line);
            }
        }
        LOGGER.info("wrote inputData for {}", currentInput);
    }

    @Test
    public void execute() throws IOException {
        // given:
        DataIdentifier outputIdentifier;

        try (ExampleBatchJob instance = new ExampleBatchJob(dataResolverRegistry, TODAY)) {
            // when:
            instance.init(sparkSession);
            outputIdentifier = instance.getOutputIdentifier();
            instance.execute();
        }

        // then:
        DataResolver dataResolver = dataResolverRegistry.getResolverFor(DataLocation.OUTPUT);
        DataInstance outputInstance = dataResolver.createCurrentInstance(outputIdentifier);
        String[] result = readBatchLines(outputInstance);
        assertArrayEquals(OUTPUT_DATA, result);
    }
}
