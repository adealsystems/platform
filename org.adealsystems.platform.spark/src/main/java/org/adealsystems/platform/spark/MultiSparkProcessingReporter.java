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

package org.adealsystems.platform.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class MultiSparkProcessingReporter implements SparkProcessingReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiSparkProcessingReporter.class);

    private final List<SparkProcessingReporter> sparkProcessingReporters;

    public MultiSparkProcessingReporter(List<SparkProcessingReporter> sparkProcessingReporters) {
        this.sparkProcessingReporters = Objects.requireNonNull(sparkProcessingReporters, "sparkProcessingReporters must not be null!");
        if(sparkProcessingReporters.isEmpty()) {
            throw new IllegalArgumentException("sparkProcessingReporters must not be empty!");
        }
        for(SparkProcessingReporter current : sparkProcessingReporters) {
            if(current == null) {
                throw new IllegalArgumentException("sparkProcessingReporters must not contain null!");
            }
        }
    }

    @Override
    public void reportSuccess(SparkDataProcessingJob job, LocalDateTime timestamp, long processingDuration) {
        for(SparkProcessingReporter current : sparkProcessingReporters) {
            try {
                current.reportSuccess(job, timestamp, processingDuration);
            } catch (Throwable t) {
                LOGGER.error("Exception while reporting success with {}!", current, t);
            }
        }
    }

    @Override
    public void reportFailure(SparkDataProcessingJob job, LocalDateTime timestamp, long processingDuration, Throwable cause) {
        for(SparkProcessingReporter current : sparkProcessingReporters) {
            try {
                current.reportFailure(job, timestamp, processingDuration, cause);
            } catch (Throwable t) {
                LOGGER.error("Exception while reporting failure with {}!", current, t);
            }
        }
    }
}
