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

package org.adealsystems.platform.state

import spock.lang.Specification

class ProcessingStateSpec extends Specification {

    def "creating failed state works for string array"() {
        when:
        ProcessingState result = ProcessingState.createFailedState("error")

        then:
        result.errors == ["error"] as List
        result.hasErrors()
    }

    def "creating failed state works for string list"() {
        when:
        ProcessingState result = ProcessingState.createFailedState(["error"] as List)

        then:
        result.errors == ["error"] as List
        result.hasErrors()
    }

    def "creating failed state with null array fails as expected"() {
        when:
        ProcessingState.createFailedState((String[]) null)

        then:
        NullPointerException ex = thrown()
        ex.message == "errors must not be null!"
    }

    def "creating failed state with null list fails as expected"() {
        when:
        ProcessingState.createFailedState((List<String>) null)

        then:
        NullPointerException ex = thrown()
        ex.message == "errors must not be null!"
    }

    def "creating failed state with empty array fails as expected"() {
        when:
        ProcessingState.createFailedState((String[]) [])

        then:
        IllegalArgumentException ex = thrown()
        ex.message == "errors must not be empty!"
    }

    def "creating failed state with empty list fails as expected"() {
        when:
        ProcessingState.createFailedState((List<String>) [])

        then:
        IllegalArgumentException ex = thrown()
        ex.message == "errors must not be empty!"
    }

    def "creating state from throwable works as expected"() {
        given:
        Throwable cause = new IllegalStateException("bar")
        Throwable t = new IllegalStateException("foo", cause)

        when:
        ProcessingState state = ProcessingState.createFailedState(t)
        def errors = state.errors

        then:
        errors != null
        errors.size() == 2
        errors == [
            "java.lang.IllegalStateException: foo",
            "java.lang.IllegalStateException: bar",
        ]
    }

    def "creating state from message and throwable works as expected"() {
        given:
        Throwable cause = new IllegalStateException("bar")
        Throwable t = new IllegalStateException("foo", cause)

        when:
        ProcessingState state = ProcessingState.createFailedState("message", t)
        def errors = state.errors

        then:
        errors != null
        errors.size() == 3
        errors == [
            "message",
            "java.lang.IllegalStateException: foo",
            "java.lang.IllegalStateException: bar",
        ]
    }

    def "creating failed state with null throwable fails as expected"() {
        when:
        ProcessingState.createFailedState((Throwable) null)

        then:
        NullPointerException ex = thrown()
        ex.message == "throwable must not be null!"
    }

    def "creating failed state with null message and throwable fails as expected"() {
        when:
        ProcessingState.createFailedState(null, new IllegalStateException("foo"))

        then:
        NullPointerException ex = thrown()
        ex.message == "message must not be null!"
    }

    def "creating failed state with message and null throwable fails as expected"() {
        when:
        ProcessingState.createFailedState("message", (Throwable) null)

        then:
        NullPointerException ex = thrown()
        ex.message == "throwable must not be null!"
    }
}
