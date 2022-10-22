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

package org.adealsystems.platform.time

import spock.lang.Specification


class DurationFormatterSpec extends Specification {

    def 'DurationFormatter works as expected'() {
        when:
        def value = DurationFormatter.fromMillis(millis).format(format)

        then:
        value == expected

        where:
        millis                                                                        | format           | expected
        100                                                                           | '%%%S %'         | '%100 %'
        100                                                                           | '%d:%H:%m:%s:%S' | '0:0:0:0:100'
        999                                                                           | '%d:%H:%m:%s:%S' | '0:0:0:0:999'
        1000                                                                          | '%d:%H:%m:%s:%S' | '0:0:0:1:0'
        1001                                                                          | '%d:%H:%m:%s:%S' | '0:0:0:1:1'
        15 * 1000                                                                     | '%d:%H:%m:%s:%S' | '0:0:0:15:0'
        15 * 1000 + 70                                                                | '%d:%H:%m:%s:%S' | '0:0:0:15:70'
        3 * 60 * 1000 + 15 * 1000 + 70                                                | '%d:%H:%m:%s:%S' | '0:0:3:15:70'
        5 * 60 * 60 * 1000 + 3 * 60 * 1000 + 15 * 1000 + 70                           | '%d:%H:%m:%s:%S' | '0:5:3:15:70'
        7 * 24 * 60 * 60 * 1000 + 5 * 60 * 60 * 1000 + 3 * 60 * 1000 + 15 * 1000 + 70 | '%d:%H:%m:%s:%S' | '7:5:3:15:70'

        5 * 60 * 60 * 1000 + 15 * 60 * 1000 + 15 * 1000 + 70                          | '%Hh %mm'        | '5h 15m'
        5 * 60 * 60 * 1000 + 15 * 60 * 1000 + 15 * 1000 + 70                          | '%hh %mm'        | '5h 15m'
        5 * 60 * 60 * 1000 + 15 * 60 * 1000 + 15 * 1000 + 70                          | '%h:%m %t'       | '5:15 am'
        17 * 60 * 60 * 1000 + 15 * 60 * 1000 + 15 * 1000 + 70                         | '%h:%m %t'       | '5:15 pm'
        17 * 60 * 60 * 1000 + 15 * 60 * 1000 + 15 * 1000 + 70                         | '%H:%m'          | '17:15'
    }

    def 'DurationFormatter with length pattern works as expected'() {
        when:
        def value = DurationFormatter.fromMillis(millis).format(format)

        then:
        value == expected

        where:
        millis | format               | expected
        100    | '%d:%2H:%2m:%2s:%3S' | '0:00:00:00:100'
        999    | '%d:%2H:%2m:%2s:%3S' | '0:00:00:00:999'
        1      | '%d:%2H:%2m:%2s:%3S' | '0:00:00:00:001'
    }
}
