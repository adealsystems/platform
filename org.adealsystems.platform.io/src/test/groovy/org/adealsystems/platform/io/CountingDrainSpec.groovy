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

package org.adealsystems.platform.io

import spock.lang.Specification

class CountingDrainSpec extends Specification {
    def "add and addAll work as expected"() {
        given:
        ListDrain<Integer> innerDrain = new ListDrain<>()
        CountingDrain<Integer> instance = new CountingDrain<>(innerDrain)

        when:
        instance.add(1)
        instance.add(2)
        and:
        instance.addAll([3, 4])
        instance.addAll([5, 6])

        then:
        innerDrain.content == [1, 2, 3, 4, 5, 6]
        instance.counter == 6
    }

    def 'add(..) throws exception if already closed'() {
        given:
        ListDrain<Integer> innerDrain = new ListDrain<>()
        CountingDrain<Integer> instance = new CountingDrain<>(innerDrain)

        when:
        instance.close()
        and:
        instance.add(1)

        then:
        DrainException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'broken drain throws expected exceptions'() {
        given:
        Drain<Integer> innerDrain = new BrokenDrain<>()
        CountingDrain<Integer> instance = new CountingDrain<>(innerDrain)

        when:
        instance.add(1)

        then:
        UnsupportedOperationException innerEx = thrown()
        innerEx.message == "Nope."

        when:
        instance.close()

        then:
        DrainException ex = thrown()
        ex.message == "Exception while closing innerDrain!"
        ex.cause instanceof UnsupportedOperationException
        ex.cause.message == "Nope."
    }

    def "closing twice is ok"() {
        given:
        Drain<String> instance = new CountingDrain<>(new ListDrain<>())

        when:
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }
}
