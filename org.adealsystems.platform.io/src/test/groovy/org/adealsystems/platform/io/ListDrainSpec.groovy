/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

package org.adealsystems.platform.io

import spock.lang.Specification

class ListDrainSpec extends Specification {
    def 'adding to the drain works'() {
        given:
        ListDrain<String> instance = new ListDrain<>()

        when:
        instance.add("Entry 1")
        and:
        instance.addAll(["Entry 2", "Entry 3"])

        then:
        instance.content == ["Entry 1", "Entry 2", "Entry 3"]
    }

    def 'add(..) throws exception if already closed'() {
        given:
        ListDrain<String> instance = new ListDrain<>()

        when:
        instance.close()
        and:
        instance.add("Entry 1")

        then:
        DrainException ex = thrown()
        ex.message == "Drain was already closed!"

        when:
        instance.reset()
        instance.add("Entry 1")

        then:
        noExceptionThrown()
    }

    def 'addAll(..) throws exception if already closed'() {
        given:
        ListDrain<String> instance = new ListDrain<>()

        when:
        instance.close()
        and:
        instance.addAll(["Entry 2", "Entry 3"])

        then:
        DrainException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'add(null) throws exception'() {
        given:
        ListDrain<String> instance = new ListDrain<>()

        when:
        instance.add(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entry must not be null!"
    }

    def 'addAll(null) throws exception'() {
        given:
        ListDrain<String> instance = new ListDrain<>()

        when:
        instance.addAll(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not be null!"
    }

    def 'addAll([null]) throws exception'() {
        given:
        ListDrain<String> instance = new ListDrain<>()

        when:
        instance.addAll([null])

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not contain null!"
    }

    def "closing twice is ok"() {
        given:
        ListDrain<String> instance = new ListDrain<>()

        when:
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def "toString returns expected value"() {
        given:
        ListDrain<String> instance = new ListDrain<>()

        when:
        instance.add('A')
        instance.add('B')

        then:
        instance.toString() == "ListDrain{content=[A, B]}"
    }
}
