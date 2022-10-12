/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

class CountingWellSpec extends Specification {
    def "iteration works as expected"() {
        given:
        ListWell<Integer> innerWell = new ListWell<>([1, 2, 3, 4])
        CountingWell<Integer> instance = new CountingWell<>(innerWell)

        expect:
        !instance.isConsumed()

        when: 'iterator is obtained'
        Iterator<Integer> iterator = instance.iterator()
        then:
        instance.isConsumed()

        when: 'content of iterator is collected into a list'
        List<Integer> collected = new ArrayList<>()
        iterator.forEachRemaining(collected::add)

        then: 'the list contains the expected values'
        collected == [1, 2, 3, 4]
        instance.counter == 4
    }

    def "creation with null innerDrain throws expected exception"() {
        when:
        new CountingWell<>(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "innerWell must not be null!"
    }

    def "creating iterator for closed instance causes expected exception"() {
        given:
        Well<String> instance = new CountingWell<>(new ListWell<>())

        when:
        instance.close()
        instance.iterator()

        then:
        WellException ex = thrown()
        ex.message == "Well was already closed!"
    }

    def "iterating iterator for closed instance causes expected exception"() {
        given:
        ListWell<Integer> innerWell = new ListWell<>([1, 2, 3, 4])
        Well<Integer> instance = new CountingWell<>(innerWell)

        when:
        def iterator = instance.iterator()
        instance.close()
        then:
        instance.consumed

        when:
        iterator.next()

        then:
        WellException ex = thrown()
        ex.message == "Well was already closed!"
    }

    def 'broken well throws expected exceptions'() {
        given:
        Well<Integer> innerWell = new BrokenWell<>()
        CountingWell<Integer> instance = new CountingWell<>(innerWell)

        when:
        def iterator = instance.iterator()
        iterator.next()

        then:
        UnsupportedOperationException innerEx = thrown()
        innerEx.message == "Nope."

        when:
        instance.close()

        then:
        WellException ex = thrown()
        ex.message == "Exception while closing innerWell!"
        ex.cause instanceof UnsupportedOperationException
        ex.cause.message == "Nope."
    }

    def "closing twice is ok"() {
        given:
        Well<String> instance = new CountingWell<>(new ListWell<>())

        when:
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }
}
