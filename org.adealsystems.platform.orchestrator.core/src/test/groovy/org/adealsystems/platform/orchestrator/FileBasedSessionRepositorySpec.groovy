package org.adealsystems.platform.orchestrator

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

import java.time.LocalDateTime

class FileBasedSessionRepositorySpec extends Specification {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSessionRepositorySpec)

    @TempDir
    File baseDirectory
    def 'instance works as expected'() {
        given:
        InstanceId instanceId = new InstanceId('0001-instance-id')
        def instanceConfiguration = [
            'aaa': 'xxx',
            'bbb': 'yyy'
        ]
        def sessionState = [
            'ccc': 'zzz',
            'ddd': '000',
        ]

        FileBasedSessionRepository instance = new FileBasedSessionRepository(instanceId, baseDirectory)
        def id = 'SESSION-ID'
        SessionId sessionId = new SessionId(id)

        when:
        def allIds = instance.retrieveSessionIds()

        then:
        allIds == [] as Set

        when:
        def session = instance.createSession(sessionId)

        then:
        session != null
        session.id == sessionId
        session.instanceConfiguration == [:]
        session.state == null

        when:
        allIds = instance.retrieveSessionIds()

        then:
        allIds == [sessionId] as Set

        when:
        def sessionWithConfig = new Session(instanceId, session.getId(), LocalDateTime.of(2023, 3,24,0,0,0,0), instanceConfiguration)
        sessionWithConfig.setState(sessionState)
        instance.updateSession(sessionWithConfig)
        def otherSession = instance.retrieveSession(sessionId)

        then:
        otherSession.present
        otherSession.get().id == sessionId
        otherSession.get().instanceConfiguration == instanceConfiguration
        otherSession.get().state == sessionState

        when:
        def lines = readLines(instance, sessionId)

        then:
        lines == [
            '{',
            '  "instanceId" : "0001-instance-id",',
            '  "id" : "SESSION-ID",',
            '  "creationTimestamp" : [ 2023, 3, 24, 0, 0 ],',
            '  "instanceConfiguration" : {',
            '    "aaa" : "xxx",',
            '    "bbb" : "yyy"',
            '  },',
            '  "state" : {',
            '    "ccc" : "zzz",',
            '    "ddd" : "000"',
            '  }',
            '}'
        ]

        when:
        def existingSession = instance.retrieveOrCreateSession(sessionId)

        then:
        existingSession.id == sessionId
        existingSession.instanceConfiguration == instanceConfiguration
        existingSession.state == sessionState

        when:
        def deleted = instance.deleteSession(sessionId)

        then:
        deleted

        when:
        def oneMoreSession = instance.retrieveSession(sessionId)

        then:
        !oneMoreSession.present

        when:
        def freshSession = instance.retrieveOrCreateSession(sessionId)

        then:
        freshSession.id == sessionId
        freshSession.instanceConfiguration == [:]
        freshSession.state == null
    }

    private static List<String> readLines(FileBasedSessionRepository instance, SessionId id) {
        File file = instance.getSessionFile(id)
        LOGGER.info("Reading lines from '{}'", file.getAbsolutePath())
        return file.readLines()
    }
}
