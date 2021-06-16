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

package org.adealsystems.platform.spark

import spock.lang.Specification

import java.time.LocalDateTime

class MultiSparkProcessingReporterSpec extends Specification {
    def "null throws expected exception"() {
        when:
        new MultiSparkProcessingReporter(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "sparkProcessingReporters must not be null!"
    }

    def "contained null throws expected exception"() {
        when:
        new MultiSparkProcessingReporter([null])

        then:
        IllegalArgumentException ex = thrown()
        ex.message == "sparkProcessingReporters must not contain null!"
    }

    def "contained reporters reportSuccess is called regardless of exception in other contained reporters"() {
        given:
        def counting = new CountingReporter()
        def failing = new FailingReporter()
        def contained = [counting, failing, counting, failing]
        def instance = new MultiSparkProcessingReporter(contained)

        when:
        instance.reportSuccess(null, null, 0)

        then:
        counting.success == 2
        counting.failure == 0
    }

    def "contained reporters reportFailure is called regardless of exception in other contained reporters"() {
        given:
        def counting = new CountingReporter()
        def failing = new FailingReporter()
        def contained = [counting, failing, counting, failing]
        def instance = new MultiSparkProcessingReporter(contained)

        when:
        instance.reportFailure(null, null, 0, null)

        then:
        counting.success == 0
        counting.failure == 2
    }

    static class CountingReporter implements SparkProcessingReporter {
        int success
        int failure

        @Override
        void reportSuccess(SparkDataProcessingJob job, LocalDateTime timestamp, long processingDuration) {
            success++
        }

        @Override
        void reportFailure(SparkDataProcessingJob job, LocalDateTime timestamp, long processingDuration, Throwable cause) {
            failure++
        }

        @Override
        String toString() {
            return "CountingReporter{" +
                "success=" + success +
                ", failure=" + failure +
                '}'
        }
    }

    static class FailingReporter implements SparkProcessingReporter {
        @Override
        void reportSuccess(SparkDataProcessingJob job, LocalDateTime timestamp, long processingDuration) {
            throw new IllegalStateException("Epic Fail!")
        }

        @Override
        void reportFailure(SparkDataProcessingJob job, LocalDateTime timestamp, long processingDuration, Throwable cause) {
            throw new IllegalStateException("Epic Fail!")
        }

        @Override
        String toString() {
            return "FailingReporter{}"
        }
    }
}
