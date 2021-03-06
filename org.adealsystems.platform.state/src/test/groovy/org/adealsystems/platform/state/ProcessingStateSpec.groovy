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

package org.adealsystems.platform.state

import spock.lang.Specification

class ProcessingStateSpec extends Specification {

    def "creating failed state works for string array"() {
        when:
        ProcessingState result = ProcessingState.getFailedState("error")

        then:
        result.errors == ["error"] as List
        result.hasErrors()
    }

    def "creating failed state works for string list"() {
        when:
        ProcessingState result = ProcessingState.getFailedState(["error"] as List)

        then:
        result.errors == ["error"] as List
        result.hasErrors()
    }

    def "creating failed state with null array fails as expected"() {
        when:
        ProcessingState.getFailedState((String[]) null)

        then:
        NullPointerException ex = thrown()
        ex.message == "errors must not be null!"
    }

    def "creating failed state with null list fails as expected"() {
        when:
        ProcessingState.getFailedState((List<String>) null)

        then:
        NullPointerException ex = thrown()
        ex.message == "errors must not be null!"
    }

    def "creating failed state with empty array fails as expected"() {
        when:
        ProcessingState.getFailedState((String[]) [])

        then:
        IllegalArgumentException ex = thrown()
        ex.message == "errors must not be empty!"
    }

    def "creating failed state with empty list fails as expected"() {
        when:
        ProcessingState.getFailedState((List<String>) [])

        then:
        IllegalArgumentException ex = thrown()
        ex.message == "errors must not be empty!"
    }
}
