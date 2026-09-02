/*
 * Copyright 2020-2026 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator

import org.springframework.context.ApplicationContext
import spock.lang.Specification

class DynamicContentInstanceEventHandlerSpec extends Specification {

    def 'handler obtained as expected'() {
        given:
        ApplicationContext applicationContext = Mock()
        when:
        var handler = DynamicContentInstanceEventHandler.forHandler(
            applicationContext,
            TestDynamicContentAwareHandler.class
        )
        then:
        handler != null

        when:
        var otherHandler = DynamicContentInstanceEventHandler.forHandler(
            applicationContext,
            TestDynamicContentAwareHandler.class
        )

        then: 'handlers are the same instance'
        handler === otherHandler
        and: 'no methods on any mocks have been called so far'
        0 * _
    }

    private static class TestDynamicContentAwareHandler implements DynamicContentAwareHandler {

        @Override
        InstanceId getInstanceId() {
            return null
        }

        @Override
        String getDynamicContent() {
            return null
        }

        @Override
        void setDynamicContent(String dynamicContent) {

        }

        @Override
        InternalEvent handle(InternalEvent event, Session session) {
            return null
        }

        @Override
        boolean isRelevant(InternalEvent event) {
            return false
        }

        @Override
        Optional<Long> getTimeout() {
            return null
        }

        @Override
        boolean isSessionStartEvent(InternalEvent event) {
            return false
        }

        @Override
        boolean isSessionStopEvent(InternalEvent event, Session session) {
            return false
        }

        @Override
        Optional<String> determineDynamicContent(InternalEvent event) {
            return null
        }

        @Override
        Optional<RunSpecification> getCurrentRun() {
            return null
        }

        @Override
        boolean isValid(InternalEvent event) {
            return false
        }

        @Override
        boolean isTerminating(InternalEvent event) {
            return false
        }

        @Override
        void resetTerminatingFlag(InternalEvent event) {

        }
    }
}
