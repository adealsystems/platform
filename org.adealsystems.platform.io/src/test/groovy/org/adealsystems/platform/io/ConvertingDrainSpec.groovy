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

class ConvertingDrainSpec extends Specification {
    def "add and addAll work as expected"() {
        given:
        ListDrain<String> innerDrain = new ListDrain<>()
        ConvertingDrain<Integer, String> instance = new ConvertingDrain<>(innerDrain, String::valueOf)

        when:
        instance.add(1)
        instance.add(2)
        and:
        instance.addAll([3, 4])
        instance.addAll([5, 6])

        then:
        innerDrain.content == ["1", "2", "3", "4", "5", "6"]
    }

    def 'add(null) throws exception'() {
        given:
        ListDrain<String> innerDrain = new ListDrain<>()
        ConvertingDrain<Integer, String> instance = new ConvertingDrain<>(innerDrain, String::valueOf)

        when:
        instance.add(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entry must not be null!"
    }

    def 'addAll(null) throws exception'() {
        given:
        ListDrain<String> innerDrain = new ListDrain<>()
        ConvertingDrain<Integer, String> instance = new ConvertingDrain<>(innerDrain, String::valueOf)

        when:
        instance.addAll(null)

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not be null!"
    }

    def 'addAll([null]) throws exception'() {
        given:
        ListDrain<String> innerDrain = new ListDrain<>()
        ConvertingDrain<Integer, String> instance = new ConvertingDrain<>(innerDrain, String::valueOf)

        when:
        instance.addAll([null])

        then:
        NullPointerException ex = thrown()
        ex.message == "entries must not contain null!"
    }

    def "creation with null innerDrain throws expected exception"() {
        when:
        new ConvertingDrain<>(null, String::valueOf)

        then:
        NullPointerException ex = thrown()
        ex.message == "innerDrain must not be null!"
    }

    def "creation with null convertFunction throws expected exception"() {
        when:
        new ConvertingDrain<>(new ListDrain<>(), null)

        then:
        NullPointerException ex = thrown()
        ex.message == "convertFunction must not be null!"
    }
}
