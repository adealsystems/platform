/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

package org.adealsystems.platform.time

import spock.lang.Specification


class DurationFormatterSpec extends Specification {

    def 'Basics of DurationFormatter working as expected'() {
        when:
        def value = DurationFormatter.fromMillis(millis).format(format)

        then:
        value == expected

        where:
        millis | format    | expected
        1000   | '%S'      | '1000'
        1000   | '%s'      | '1'
        1000   | '%2s'     | '01'
        999    | '%s.%S'   | '0.999'
        1000   | '%s.%S'   | '1.0'
        1001   | '%s.%S'   | '1.1'
        1001   | '%2s.%3S' | '01.001'
        1001   | '%S'      | '1001'
        1001   | '%s'      | '1'

        100    | '%%%S %'  | '%100 %'
    }

    def 'DurationFormatter works as expected'() {
        when:
        def value = DurationFormatter.fromMillis(millis).format(format)

        then:
        value == expected

        where:
        millis                                                                        | format           | expected
        100                                                                           | '%d:%h:%m:%s:%S' | '0:0:0:0:100'
        999                                                                           | '%d:%h:%m:%s:%S' | '0:0:0:0:999'
        1000                                                                          | '%d:%h:%m:%s:%S' | '0:0:0:1:0'
        1001                                                                          | '%d:%h:%m:%s:%S' | '0:0:0:1:1'
        15 * 1000                                                                     | '%d:%h:%m:%s:%S' | '0:0:0:15:0'
        15 * 1000 + 70                                                                | '%d:%h:%m:%s:%S' | '0:0:0:15:70'
        3 * 60 * 1000 + 15 * 1000 + 70                                                | '%d:%h:%m:%s:%S' | '0:0:3:15:70'
        5 * 60 * 60 * 1000 + 3 * 60 * 1000 + 15 * 1000 + 70                           | '%d:%h:%m:%s:%S' | '0:5:3:15:70'
        7 * 24 * 60 * 60 * 1000 + 5 * 60 * 60 * 1000 + 3 * 60 * 1000 + 15 * 1000 + 70 | '%d:%h:%m:%s:%S' | '7:5:3:15:70'

        5 * 60 * 60 * 1000 + 15 * 60 * 1000 + 15 * 1000 + 70                          | '%hh %mm'        | '5h 15m'
        17 * 60 * 60 * 1000 + 15 * 60 * 1000 + 15 * 1000 + 70                         | '%h:%m'          | '17:15'
    }

    def 'DurationFormatter with length pattern works as expected'() {
        when:
        def value = DurationFormatter.fromMillis(millis).format(format)

        then:
        value == expected

        where:
        millis | format               | expected
        100    | '%d:%2h:%2m:%2s:%3S' | '0:00:00:00:100'
        999    | '%d:%2h:%2m:%2s:%3S' | '0:00:00:00:999'
        1      | '%d:%2h:%2m:%2s:%3S' | '0:00:00:00:001'
    }
}
