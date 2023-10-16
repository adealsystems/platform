/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

import java.util.List;
import java.util.Objects;

public class MultiJobFinalizer implements JobFinalizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiJobFinalizer.class);

    private final List<JobFinalizer> jobFinalizers;

    public MultiJobFinalizer(List<JobFinalizer> jobFinalizers) {
        this.jobFinalizers = Objects.requireNonNull(jobFinalizers, "jobFinalizers must not be null!");
        if (jobFinalizers.isEmpty()) {
            throw new IllegalArgumentException("jobFinalizers must not be empty!");
        }
        for (JobFinalizer jobFinalizer : jobFinalizers) {
            if (jobFinalizer == null) {
                throw new IllegalArgumentException("jobFinalizers must not contain null!");
            }
        }
    }

    @Override
    public void finalizeJob(SparkDataProcessingJob job) {
        Objects.requireNonNull(job, "job must not be null!");

        for (JobFinalizer jobFinalizer : jobFinalizers) {
            try {
                jobFinalizer.finalizeJob(job);
            } catch (Throwable th) {
                LOGGER.error("Exception while reporting success with {}!", jobFinalizer, th);
            }
        }
    }
}
