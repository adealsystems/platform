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

package org.adealsystems.platform.io.transform

import spock.lang.Specification

class TransformerMetricsSpec extends Specification {
    def "adding works as expected"() {
        given:
        TransformerMetrics instance = new TransformerMetrics()
        instance.readEntries = 1
        instance.writtenEntries = 1
        instance.skippedEntries = 1
        instance.totalInputs = 1
        instance.transformedInputs = 1
        instance.skippedInputs = 1
        instance.conversionErrors = 1
        instance.readErrors = 1
        instance.writeErrors = 1
        instance.inputErrors = 1
        instance.inputIterationErrors = 1

        TransformerMetrics other = new TransformerMetrics()
        other.readEntries = 2
        other.writtenEntries = 3
        other.skippedEntries = 4
        other.totalInputs = 5
        other.transformedInputs = 6
        other.skippedInputs = 7
        other.conversionErrors = 8
        other.readErrors = 9
        other.writeErrors = 10
        other.inputErrors = 11
        other.inputIterationErrors = 12

        when: 'other is added to instance'
        instance.add(other)

        then: 'instance contains the added result'
        instance.readEntries == 3
        instance.writtenEntries == 4
        instance.skippedEntries == 5
        instance.totalInputs == 6
        instance.transformedInputs == 7
        instance.skippedInputs == 8
        instance.conversionErrors == 9
        instance.readErrors == 10
        instance.writeErrors == 11
        instance.inputErrors == 12
        instance.inputIterationErrors == 13

        and: 'other has not changed'
        other.readEntries == 2
        other.writtenEntries == 3
        other.skippedEntries == 4
        other.totalInputs == 5
        other.transformedInputs == 6
        other.skippedInputs == 7
        other.conversionErrors == 8
        other.readErrors == 9
        other.writeErrors == 10
        other.inputErrors == 11
        other.inputIterationErrors == 12
    }

    def "sanity checks"() {
        given:
        TransformerMetrics instance = new TransformerMetrics()
        and:
        instance.readEntries = readEntries
        instance.writtenEntries = writtenEntries
        instance.skippedEntries = skippedEntries
        instance.totalInputs = totalInputs
        instance.transformedInputs = transformedInputs
        instance.skippedInputs = skippedInputs
        instance.conversionErrors = conversionErrors
        instance.readErrors = readErrors
        instance.writeErrors = writeErrors
        instance.inputErrors = inputErrors
        instance.inputIterationErrors = inputIterationErrors

        expect:
        instance.readEntries == readEntries
        instance.writtenEntries == writtenEntries
        instance.skippedEntries == skippedEntries
        instance.totalInputs == totalInputs
        instance.transformedInputs == transformedInputs
        instance.skippedInputs == skippedInputs
        instance.conversionErrors == conversionErrors
        instance.readErrors == readErrors
        instance.writeErrors == writeErrors
        instance.inputErrors == inputErrors
        instance.inputIterationErrors == inputIterationErrors

        and:
        instance.hasErrors() == hasErrors
        def defaultInstance = new TransformerMetrics()
        if (equal) {
            assert defaultInstance == instance
            instance.hashCode() == 0
        } else {
            assert defaultInstance != instance
            instance.hashCode() != 0
        }

        where:
        readEntries | writtenEntries | skippedEntries | totalInputs | transformedInputs | skippedInputs | conversionErrors | readErrors | writeErrors | inputErrors | inputIterationErrors | equal | hasErrors
        0           | 0              | 0              | 0           | 0                 | 0             | 0                | 0          | 0           | 0           | 0                    | true  | false
        1           | 0              | 0              | 0           | 0                 | 0             | 0                | 0          | 0           | 0           | 0                    | false | false
        0           | 1              | 0              | 0           | 0                 | 0             | 0                | 0          | 0           | 0           | 0                    | false | false
        0           | 0              | 1              | 0           | 0                 | 0             | 0                | 0          | 0           | 0           | 0                    | false | false
        0           | 0              | 0              | 1           | 0                 | 0             | 0                | 0          | 0           | 0           | 0                    | false | false
        0           | 0              | 0              | 0           | 1                 | 0             | 0                | 0          | 0           | 0           | 0                    | false | false
        0           | 0              | 0              | 0           | 0                 | 1             | 0                | 0          | 0           | 0           | 0                    | false | false
        0           | 0              | 0              | 0           | 0                 | 0             | 1                | 0          | 0           | 0           | 0                    | false | true
        0           | 0              | 0              | 0           | 0                 | 0             | 0                | 1          | 0           | 0           | 0                    | false | true
        0           | 0              | 0              | 0           | 0                 | 0             | 0                | 0          | 1           | 0           | 0                    | false | true
        0           | 0              | 0              | 0           | 0                 | 0             | 0                | 0          | 0           | 1           | 0                    | false | true
        0           | 0              | 0              | 0           | 0                 | 0             | 0                | 0          | 0           | 0           | 1                    | false | true
    }

    @SuppressWarnings(['ChangeToOperator', 'GrEqualsBetweenInconvertibleTypes'])
    def "edge cases"() {
        given:
        TransformerMetrics instance = new TransformerMetrics()

        expect:
        instance.equals(instance)
        !instance.equals(null)
        !instance.equals("foo")
        instance.toString() != null // :p
    }

    def "adding null throws expected exception"() {
        given:
        TransformerMetrics instance = new TransformerMetrics()

        when:
        instance.add(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "metrics must not be null!"
    }
}
