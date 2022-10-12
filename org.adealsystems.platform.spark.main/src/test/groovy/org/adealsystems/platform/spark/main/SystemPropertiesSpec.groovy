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

package org.adealsystems.platform.spark.main

import spock.lang.Specification
import spock.util.environment.RestoreSystemProperties

class SystemPropertiesSpec extends Specification {

    private static final String TEST_PROPERTY_NAME = 'the.property'
    private static final String TEST_PROPERTIES_PATH = 'test-replacement.properties'
    private static final String EXISTING_REPLACEMENT = 'foo'
    private static final String MISSING_REPLACEMENT = 'missing'

    @RestoreSystemProperties
    def "existing replacement is performed"() {
        given:
        System.setProperty(TEST_PROPERTY_NAME, EXISTING_REPLACEMENT)

        when:
        SystemProperties.replaceProperty(TEST_PROPERTY_NAME, TEST_PROPERTIES_PATH)

        then:
        'bar,baz' == System.getProperty(TEST_PROPERTY_NAME)
    }

    @RestoreSystemProperties
    def "missing replacement does not replace"() {
        given:
        System.setProperty(TEST_PROPERTY_NAME, MISSING_REPLACEMENT)

        when:
        SystemProperties.replaceProperty(TEST_PROPERTY_NAME, TEST_PROPERTIES_PATH)

        then:
        MISSING_REPLACEMENT == System.getProperty(TEST_PROPERTY_NAME)
    }

    @RestoreSystemProperties
    def "missing properties won't hurt"() {
        given:
        System.setProperty(TEST_PROPERTY_NAME, EXISTING_REPLACEMENT)

        when:
        SystemProperties.replaceProperty(TEST_PROPERTY_NAME, "missing.properties")

        then:
        EXISTING_REPLACEMENT == System.getProperty(TEST_PROPERTY_NAME)
    }
}
