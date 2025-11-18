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

import org.adealsystems.platform.orchestrator.status.FileProcessingStep
import org.adealsystems.platform.orchestrator.status.SessionProcessingState
import org.adealsystems.platform.orchestrator.status.State
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

import java.time.LocalDateTime

class SynchronizedFileBasedSessionRepositorySpec extends Specification {
    @TempDir
    File baseDirectory

    @Shared
    def instanceId = new InstanceId('0001-instance-id')

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

    /*
- stored: 	Session{instanceId=0088-arms-package-optimization-schedule, id=01KABBQRX5T0RR3ABY0WFKQDA1, creationTimestamp=2025-11-18T12:31:27.274351003, instanceConfiguration={}, state={update-history=[raw-event-handler] 12:31:27.338: org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.startSession(InternalEventHandlerRunnable.java:884), [event-handler-0088-arms-package-optimization-schedule] 12:31:27.368: org.adealsystems.platform.orchestrator.InstanceEventHandlerRunnable.run(InstanceEventHandlerRunnable.java:148), __session_category=phase-1, __run_id=2025-11-18}, processingState=SessionProcessingState{runSpec=Run{type=ACTIVE, id='2025-11-18'}, configuration=null, state=RUNNING, message=null, 					steps=[], 																																									started=2025-11-18T12:31:27.299368488, terminated=null, lastUpdated=2025-11-18T12:31:27.368152591, progressMaxValue=4, progressCurrentStep=0, progressFailedSteps=0, flags={ERROR_OCCURRED=false, SESSION_FINISHED=false, SESSION_CANCELLED=false}, stateAttributes={}}}
- modified: Session{instanceId=0088-arms-package-optimization-schedule, id=01KABBQRX5T0RR3ABY0WFKQDA1, creationTimestamp=2025-11-18T12:31:27.274351003, instanceConfiguration={}, state={update-history=[raw-event-handler] 12:31:27.338: org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.startSession(InternalEventHandlerRunnable.java:884), [event-handler-0088-arms-package-optimization-schedule] 12:31:27.368: org.adealsystems.platform.orchestrator.InstanceEventHandlerRunnable.run(InstanceEventHandlerRunnable.java:148), __session_category=phase-1, __run_id=2025-11-18}, processingState=SessionProcessingState{runSpec=Run{type=ACTIVE, id='2025-11-18'}, configuration=null, state=RUNNING, message=Waiting for collector, 	steps=[FileProcessingStep{zone=INCOMING, metaName='arms_package_optimization_schedule'}], 																					started=2025-11-18T12:31:27.299368488, terminated=null, lastUpdated=2025-11-18T12:31:27.412461352, progressMaxValue=4, progressCurrentStep=1, progressFailedSteps=0, flags={ERROR_OCCURRED=false, SESSION_FINISHED=false, SESSION_CANCELLED=false}, stateAttributes={update-history=[raw-event-handler] 12:31:27.338: org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.startSession(InternalEventHandlerRunnable.java:884), [event-handler-0088-arms-package-optimization-schedule] 12:31:27.368: org.adealsystems.platform.orchestrator.InstanceEventHandlerRunnable.run(InstanceEventHandlerRunnable.java:148), __session_category=phase-1, __run_id=2025-11-18}}}
     */
    def 'concurrent update modifications (special case) are working as expected'() {
        given:
        def instanceId = new InstanceId('0088-arms-package-optimization-schedule')
        def sessionId = new SessionId('01KABBQRX5T0RR3ABY0WFKQDA1')
        SynchronizedFileBasedSessionRepository repo
            = new SynchronizedFileBasedSessionRepository(instanceId, baseDirectory)
        def ts = LocalDateTime.of(2025, 11,18,12,31,27,0)
        def state = [:]
        state.put('update-history', '[raw-event-handler] 12:31:27.338: org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.startSession(InternalEventHandlerRunnable.java:884), [event-handler-0088-arms-package-optimization-schedule] 12:31:27.368: org.adealsystems.platform.orchestrator.InstanceEventHandlerRunnable.run(InstanceEventHandlerRunnable.java:148)')
        state.put('__session_category', 'phase-1')
        state.put('__run_id', '2025-11-18')
        def runSpec = new RunSpecification(RunType.ACTIVE, '2025-11-18')
        def unloadingPointFileProcessingStep = new FileProcessingStep(true, new InternalEvent(), null, 'INCOMING', 'arms_package_optimization_schedule')
        def processingState1 = new SessionProcessingState(runSpec, [:], State.RUNNING, null, null, null, null, 4, 0, 0, [:], [], [:])
        def processingState2 = new SessionProcessingState(runSpec, [:], State.RUNNING, 'Waiting for collector', null, null, null, 4, 0, 0, [:], [unloadingPointFileProcessingStep], [:])

        when:
        def session1 = repo.createSession(sessionId, ts, [:])
        session1.setStateValue('__session_category', 'phase-1')
        session1.setStateValue('__run_id', '2025-11-18')
        session1.updateProcessingState(processingState1)

        and:
        def session2 = Session.copyOf(session1)
        session2.updateMessage('Waiting for collector')
        session2.addProcessingStep(unloadingPointFileProcessingStep)

        and:
        def merged = repo.internalMergeActiveSession(session2, session1)

        then:
        merged == session2
    }
}
