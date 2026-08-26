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
