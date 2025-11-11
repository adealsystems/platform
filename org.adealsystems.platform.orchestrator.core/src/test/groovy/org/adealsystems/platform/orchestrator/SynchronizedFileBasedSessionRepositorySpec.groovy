/*
 * Copyright 2020-2025 ADEAL Systems GmbH
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

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

import java.time.LocalDateTime

class SynchronizedFileBasedSessionRepositorySpec extends Specification {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedFileBasedSessionRepositorySpec)

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

        SynchronizedFileBasedSessionRepository sessionRepository = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)
        def id = 'SESSION-ID'
        def sessionId1 = new SessionId(id + '-1')

        when:
        def allIds = sessionRepository.retrieveSessionIds()

        then:
        allIds == [] as Set

        when:
        def session = sessionRepository.createSession(sessionId1)

        then:
        session != null
        session.id == sessionId1
        session.instanceConfiguration == [:]
        session.state == null

        when:
        allIds = sessionRepository.retrieveSessionIds()

        then:
        allIds == [sessionId1] as Set

        when:
        def sessionId2 = new SessionId(id + '-2')
        def ts = LocalDateTime.of(2023, 3,24,0,0,0,0)
        def sessionWithConfig = sessionRepository.createSession(sessionId2, ts, instanceConfiguration)
        sessionWithConfig.setState(sessionState)
        sessionRepository.updateSession(sessionWithConfig)

        def otherSession = sessionRepository.retrieveSession(sessionId2)

        then:
        otherSession.present
        otherSession.get().id == sessionId2
        otherSession.get().instanceConfiguration == instanceConfiguration
        otherSession.get().state == sessionState

        when:
        def lines = readLines(sessionRepository, sessionId2)

        then:
        lines == [
            '{',
            '  "instanceId" : "0001-instance-id",',
            '  "id" : "SESSION-ID-2",',
            '  "creationTimestamp" : [ 2023, 3, 24, 0, 0 ],',
            '  "instanceConfiguration" : {',
            '    "aaa" : "xxx",',
            '    "bbb" : "yyy"',
            '  },',
            '  "state" : {',
            '    "ccc" : "zzz",',
            '    "ddd" : "000"',
            '  },',
            '  "sessionUpdates" : {',
            '    "updates" : [ ]',
            '  }',
            '}'
        ]

        when:
        def existingSession = sessionRepository.retrieveOrCreateSession(sessionId2)

        then:
        existingSession.id == sessionId2
        existingSession.instanceConfiguration == instanceConfiguration
        existingSession.state == sessionState

        when:
        def deleted = sessionRepository.deleteSession(sessionId2)

        then:
        deleted

        when:
        def oneMoreSession = sessionRepository.retrieveSession(sessionId2)

        then:
        !oneMoreSession.present

        when:
        def freshSession = sessionRepository.retrieveOrCreateSession(sessionId2)

        then:
        freshSession.id == sessionId2
        freshSession.instanceConfiguration == [:]
        freshSession.state == null
    }

    def 'session updates are working as expected'() {
        given:
        InstanceId instanceId = new InstanceId('0001-instance-id')
        def ts = LocalDateTime.of(2023, 3,24,0,0,0,0)

        SynchronizedFileBasedSessionRepository sessionRepository = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)
        def id = 'SESSION-ID'
        def sessionId1 = new SessionId(id + '-1')

        when:
        def session = sessionRepository.createSession(sessionId1, ts, [:])
        session.setStateValue("a", "AAA")
        sessionRepository.updateSession(session)

        then:
        session != null
        session.id == sessionId1
        session.instanceConfiguration == [:]
        session.state == ['a': 'AAA']
        session.sessionUpdates != null
        session.sessionUpdates.updates.size() == 1

        when:
        def sessionLoaded = sessionRepository.retrieveSession(sessionId1)

        then:
        sessionLoaded.present
        sessionLoaded.get() == session
    }

    def 'concurrent update modifications are working as expected'() {
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

        SynchronizedFileBasedSessionRepository sessionRepository = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)
        def id = 'SESSION-ID'
        def sessionId = new SessionId(id)
        def ts = LocalDateTime.of(2023, 3,24,0,0,0,0)

        when:
        def session1 = sessionRepository.createSession(sessionId, ts, instanceConfiguration)

        then:
        session1 != null
        session1.id == sessionId
        session1.instanceConfiguration == instanceConfiguration
        session1.creationTimestamp == ts
        session1.state == null

        when:
        def session10 = sessionRepository.retrieveSession(sessionId).get()

        then:
        session1 == session10

        when:
        def checksum1 = session1.checksum
        session1.setStateFlag('flag-1', true)
        def session2 = sessionRepository.updateSession(session1)

        then:
        checksum1 != session2.checksum

        when:
        def checksum2 = session2.checksum
        session2.setStateValue('A', 'aaa')
        def session3 = sessionRepository.updateSession(session2)

        then:
        checksum2 != session3.checksum

        when:
        def checksum10 = session10.checksum
        session10.setStateValue('X', 'xxx')
        def session11 = sessionRepository.updateSession(session10)

        then:
        checksum10 != session11.checksum
    }

    private static List<String> readLines(SynchronizedFileBasedSessionRepository instance, SessionId id) {
        File file = instance.getSessionFile(id)
        LOGGER.info("Reading lines from '{}'", file)
        return file.readLines()
    }
}
