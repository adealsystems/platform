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

class ConvertingWellSpec extends Specification {
    def "iteration works as expected"() {
        given:
        ListWell<Integer> innerWell = new ListWell<>([1, 2, 3, 4])
        ConvertingWell<Integer, String> instance = new ConvertingWell<>(innerWell, String::valueOf)

        expect:
        !instance.isConsumed()

        when: 'iterator is obtained'
        Iterator<String> iterator = instance.iterator()
        then:
        instance.isConsumed()

        when: 'content of iterator is collected into a list'
        List<String> collected = new ArrayList<>()
        iterator.forEachRemaining(collected::add)

        then: 'the list contains the expected values'
        collected == ["1", "2", "3", "4"]
    }

    def "creation with null innerDrain throws expected exception"() {
        when:
        new ConvertingWell<>(null, String::valueOf)

        then:
        NullPointerException ex = thrown()
        ex.message == "innerWell must not be null!"
    }

    def "creation with null convertFunction throws expected exception"() {
        when:
        new ConvertingWell<>(new ListWell<>(), null)

        then:
        NullPointerException ex = thrown()
        ex.message == "convertFunction must not be null!"
    }

    def "creating iterator for closed instance causes expected exception"() {
        given:
        Well<String> instance = new ConvertingWell<>(new ListWell<>(), String::valueOf)

        when:
        instance.close()
        instance.iterator()

        then:
        WellException ex = thrown()
        ex.message == "Well was already closed!"
    }

    def "closing twice is ok"() {
        given:
        Well<String> instance = new ConvertingWell<>(new ListWell<>(), String::valueOf)

        when:
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }
}
