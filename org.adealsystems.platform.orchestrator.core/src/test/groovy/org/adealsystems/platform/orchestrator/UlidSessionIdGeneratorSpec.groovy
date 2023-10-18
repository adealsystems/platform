package org.adealsystems.platform.orchestrator

import spock.lang.Specification

class UlidSessionIdGeneratorSpec extends Specification {
    def 'generate works as expected'() {
        given:
        UlidSessionIdGenerator instance = new UlidSessionIdGenerator()

        when:
        def id = instance.generate()
        def otherId = instance.generate()

        then:
        id != otherId
    }
}
