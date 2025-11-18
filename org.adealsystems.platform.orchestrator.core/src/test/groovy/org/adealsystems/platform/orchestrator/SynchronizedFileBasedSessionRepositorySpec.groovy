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


import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

import java.time.LocalDateTime

class SynchronizedFileBasedSessionRepositorySpec extends Specification {
    @TempDir
    File baseDirectory

    @Shared
    InstanceId instanceId = new InstanceId('0001-instance-id')

    @Shared
    def sessionId = new SessionId('SESSION-ID-1')

    def 'retrieveSessionIds() and createSession() are working as expected'() {
        given:
        SynchronizedFileBasedSessionRepository repo
            = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)

        when:
        def allIds = repo.retrieveSessionIds()

        then:
        allIds == [] as Set

        when:
        def session = repo.createSession(sessionId)

        then:
        session != null
        session.id == sessionId
        session.instanceConfiguration == [:]
        session.state == null

        when:
        allIds = repo.retrieveSessionIds()

        then:
        allIds == [sessionId] as Set
    }

    def 'retrieveSession() and updateSession() are working as expected'() {
        given:
        SynchronizedFileBasedSessionRepository repo
            = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)

        def instanceConfiguration = [
            'aaa': 'xxx',
            'bbb': 'yyy'
        ]
        def sessionState = [
            'ccc': 'zzz',
            'ddd': '000',
        ]
        def ts = LocalDateTime.of(2023, 3,24,0,0,0,0)

        when:
        def sessionWithConfig = repo.createSession(sessionId, ts, instanceConfiguration)
        sessionWithConfig.setState(sessionState)
        repo.updateSession(sessionWithConfig)

        def otherSession = repo.retrieveSession(sessionId)

        then:
        otherSession.present
        otherSession.get().id == sessionId
        otherSession.get().instanceConfiguration == instanceConfiguration
        otherSession.get().state == sessionState
    }

    def 'retrieveOrCreateSession() and deleteSession() are working as expected'() {
        given:
        SynchronizedFileBasedSessionRepository repo
            = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)

        repo.createSession(sessionId)

        when:
        def existingSession = repo.retrieveOrCreateSession(sessionId)

        then:
        existingSession.id == sessionId

        when:
        def deleted = repo.deleteSession(sessionId)

        then:
        deleted

        when:
        def oneMoreSession = repo.retrieveSession(sessionId)

        then:
        !oneMoreSession.present

        when:
        def freshSession = repo.retrieveOrCreateSession(sessionId)

        then:
        freshSession.id == sessionId
        freshSession.instanceConfiguration == [:]
        freshSession.state == null
    }

    def 'session updates are working as expected'() {
        given:
        SynchronizedFileBasedSessionRepository repo
            = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)

        def ts = LocalDateTime.of(2023, 3,24,0,0,0,0)

        when:
        def session = repo.createSession(sessionId, ts, [:])
        session.setStateValue("a", "AAA")
        repo.updateSession(session)

        then:
        session != null
        session.id == sessionId
        session.instanceConfiguration == [:]
        session.state == ['a': 'AAA']
        session.sessionUpdates != null
        session.sessionUpdates.updates.size() == 1

        when:
        def sessionLoaded = repo.retrieveSession(sessionId)

        then:
        sessionLoaded.present
        sessionLoaded.get() == session
    }

    def 'concurrent update modifications are working as expected'() {
        given:
        SynchronizedFileBasedSessionRepository repo
            = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)

        def instanceConfiguration = [
            'aaa': 'xxx',
            'bbb': 'yyy'
        ]
        def ts = LocalDateTime.of(2023, 3,24,0,0,0,0)

        when:
        def session1 = repo.createSession(sessionId, ts, instanceConfiguration)
        def hash1 = session1.buildChecksum()

        and:
        def session2 = repo.retrieveSession(sessionId).get()
        def hash2 = session2.buildChecksum()

        then:
        session1 == session2

        when:
        session1.setStateFlag('flag-1', true)
        def session1_b = repo.updateSession(session1)
        def hash1_b = session1_b.buildChecksum()
        // flag-1: true

        then:
        hash1 != hash1_b

        when:
        session1_b.setStateValue('A', 'aaa')
        def session1_c = repo.updateSession(session1_b)
        def hash1_c = session1_c.buildChecksum()
        // flag-1: true
        // A: aaa

        then:
        hash1_b != hash1_c

        when:
        session2.setStateValue('X', 'xxx')
        def session2_b = repo.updateSession(session2)
        def hash2_b = session2_b.buildChecksum()
        // flag-1: true
        // A: aaa
        // X: xxx

        then:
        hash2 != hash2_b
        session2_b.state.get('flag-1') == 'true'
        session2_b.state.get('A') == 'aaa'
        session2_b.state.get('X') == 'xxx'
        session2_b.processingState == null
    }
}
