/*
 * Copyright 2020-2025 ADEAL Systems GmbH
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

package org.adealsystems.platform.spark;

import org.adealsystems.platform.process.DataProcessingJob;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.util.Map;

public interface SparkDataProcessingJob extends DataProcessingJob, AutoCloseable {

    void init(SparkSession sparkSession);

    void setInitFailure(String message);

    boolean isInitSuccessful();

    String getInitErrorMessage();

    Map<String, Object> getWriterOptions();

    void setWriterOptions(Map<String, Object> options);

    void setWriterOption(String name, Object value);

    LocalDate getInvocationDate();

    /**
     * May only throw RuntimeExceptions.
     */
    @Override
    void close();

    void finalizeJob();
}
