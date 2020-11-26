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

package org.adealsystems.platform.spark.example;

import org.adealsystems.platform.DataFormat;
import org.adealsystems.platform.DataIdentifier;
import org.adealsystems.platform.DataLocation;
import org.adealsystems.platform.DataResolverRegistry;
import org.adealsystems.platform.spark.AbstractSparkBatchJob;
import org.adealsystems.platform.spark.DatasetLogger;
import org.adealsystems.platform.spark.udf.SomethingToLocalDateStringUDF;
import org.adealsystems.platform.spark.udf.WeekOfYearOfDateUDF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.types.DataTypes;

import java.time.LocalDate;
import java.util.Locale;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

public class ExampleBatchJob extends AbstractSparkBatchJob {

    private static final DataIdentifier OUTPUT_IDENTIFIER = new DataIdentifier("example", "output", DataFormat.CSV_SEMICOLON);
    private static final DataIdentifier INPUT_IDENTIFIER = new DataIdentifier("some_exporter", "input", DataFormat.CSV_SEMICOLON);
    private static final String WEEK_OF_YEAR_UDF = "WeekOfYear";
    private static final String DATE_STRING_UDF = "DateString";

    public ExampleBatchJob(DataResolverRegistry dataResolverRegistry, LocalDate invocationDate) {
        this(dataResolverRegistry, invocationDate, null);
    }

    public ExampleBatchJob(DataResolverRegistry dataResolverRegistry, LocalDate invocationDate, String configuration) {
        super(dataResolverRegistry, DataLocation.OUTPUT, OUTPUT_IDENTIFIER.withConfiguration(configuration), invocationDate);
        setWriteMode(WriteMode.BOTH);
    }

    @Override
    protected void registerUdfs(UDFRegistration udfRegistration) {
        udfRegistration.register(WEEK_OF_YEAR_UDF, new WeekOfYearOfDateUDF(Locale.GERMANY), DataTypes.StringType);
        udfRegistration.register(DATE_STRING_UDF, new SomethingToLocalDateStringUDF(), DataTypes.StringType);
    }

    @Override
    protected void registerInputs() {
        //registerInput(DataLocation.INPUT, INPUT_IDENTIFIER, getInvocationDate());
        registerCurrentInput(DataLocation.INPUT, INPUT_IDENTIFIER);
    }

    @Override
    protected Dataset<Row> processData() {
        final DatasetLogger dsl = getDatasetLogger();

        Dataset<Row> input = readInput(INPUT_IDENTIFIER);
        dsl.showInfo("Input", input);

        Dataset<Row> result = input
                .withColumn("WeekOfYear",
                        callUDF(WEEK_OF_YEAR_UDF, col("Date")))
                .withColumn("DateString",
                        callUDF(DATE_STRING_UDF, col("Date")));

        dsl.showInfo("Result - after UDF", result);

        result = result.drop("Date");
        result = result.withColumnRenamed("DateString", "Date");
        result = result.select("Date", "WeekOfYear", "COL1", "COL2");

        dsl.showInfo("Result - after select", result);

        result = result.orderBy(col("Date"));

        dsl.showInfo("Result - after orderBy", result);

        return result;
    }
}
