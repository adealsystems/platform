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

class ListWellSpec extends Specification {
    def "iterator works as expected"() {
        given:
        def content = ["one", "two", "three"]
        ListWell<String> instance = new ListWell<>(content)

        expect:
        !instance.isConsumed()

        when:
        Iterator<String> iter = instance.iterator()
        then:
        instance.isConsumed()

        when:
        List<String> collected = []
        for (String entry : iter) {
            collected.add(entry)
        }

        then:
        content == collected
    }

    def "calling iterator() twice throws exception"() {
        given:
        def content = ["one", "two", "three"]
        ListWell<String> instance = new ListWell<>(content)

        when:
        instance.iterator()
        and:
        instance.iterator()

        then:
        WellException ex = thrown()
        ex.message == "A well can only be iterated once!"
    }
}