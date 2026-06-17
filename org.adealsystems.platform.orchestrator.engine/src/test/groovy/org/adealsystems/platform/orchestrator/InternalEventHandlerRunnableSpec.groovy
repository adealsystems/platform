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

import org.adealsystems.platform.io.Drain
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode
import org.adealsystems.platform.orchestrator.executor.ExecutorResult
import org.adealsystems.platform.orchestrator.executor.email.EmailSender
import org.adealsystems.platform.orchestrator.executor.email.EmailSenderFactory
import org.adealsystems.platform.orchestrator.executor.email.EmailType
import org.adealsystems.platform.orchestrator.executor.email.RecipientsCluster
import org.adealsystems.platform.orchestrator.status.SessionProcessingState
import org.adealsystems.platform.orchestrator.status.State
import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDate
import java.time.LocalDateTime

import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_LIFECYCLE
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_LIFECYCLE_ATTRIBUTE_NAME
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_RESUME
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_START
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_STOP

class InternalEventHandlerRunnableSpec extends Specification {

    def 'start event without active session emits SESSION_START and creates active session'() {
        given:
        InstanceId instanceId = new InstanceId('0001-demo-instance')
        SessionId nextSessionId = new SessionId('NEW-SESSION')
        List<InternalEvent> historyEvents = []
        List<InternalEvent> queuedInstanceEvents = []

        InMemoryActiveSessionIdRepository activeSessionIdRepository =
            new InMemoryActiveSessionIdRepository([:], [nextSessionId])
        InMemorySessionRepository sessionRepository =
            new InMemorySessionRepository(instanceId, [:])

        InternalEventClassifier classifier = Stub() {
            isRelevant(_ as InternalEvent) >> true
            isValid(_ as InternalEvent) >> true
            getTimeout() >> Optional.empty()
            determineDynamicContent(_ as InternalEvent) >> Optional.empty()
            getCurrentRun() >> Optional.empty()
            isSessionStartEvent(_ as InternalEvent) >> true
            isSessionStopEvent(_ as InternalEvent, _ as Session) >> false
        }

        when:
        createRunnable(
            classifier,
            createTriggerEvent(),
            instanceId,
            activeSessionIdRepository,
            sessionRepository,
            historyEvents,
            queuedInstanceEvents
        ).run()

        then:
        lifecycleAttributes(historyEvents) == [SESSION_START]
        activeSessionIdRepository.retrieveActiveSessionId(instanceId).get() == nextSessionId
        sessionRepository.retrieveSession(nextSessionId).isPresent()
        sessionRepository.retrieveSession(nextSessionId).get().getProcessingState().getState() == State.READY_TO_RUN
    }

    @Unroll
    def 'start event with active session emits #expectedLifecycle when restart=#restart'() {
        given:
        InstanceId instanceId = new InstanceId('0001-demo-instance')
        SessionId existingSessionId = new SessionId('EXISTING-SESSION')
        SessionId nextSessionId = new SessionId('NEW-SESSION')
        Session existingSession = runningSession(instanceId, existingSessionId)

        List<InternalEvent> historyEvents = []
        List<InternalEvent> queuedInstanceEvents = []

        InMemoryActiveSessionIdRepository activeSessionIdRepository =
            new InMemoryActiveSessionIdRepository([(instanceId): existingSessionId], [nextSessionId])
        InMemorySessionRepository sessionRepository =
            new InMemorySessionRepository(instanceId, [(existingSessionId): existingSession])

        InternalEventClassifier classifier = Stub() {
            isRelevant(_ as InternalEvent) >> true
            isValid(_ as InternalEvent) >> true
            getTimeout() >> Optional.empty()
            determineDynamicContent(_ as InternalEvent) >> Optional.empty()
            getCurrentRun() >> Optional.empty()
            isSessionStartEvent(_ as InternalEvent) >> true
            isSessionRestartEvent(_ as InternalEvent, _ as Session) >> restart
            isSessionStopEvent(_ as InternalEvent, _ as Session) >> false
        }

        when:
        createRunnable(
            classifier,
            createTriggerEvent(),
            instanceId,
            activeSessionIdRepository,
            sessionRepository,
            historyEvents,
            queuedInstanceEvents
        ).run()

        then:
        lifecycleAttributes(historyEvents) == expectedLifecycle
        activeSessionIdRepository.retrieveActiveSessionId(instanceId).get() == expectedActiveSessionId
        sessionRepository.sessions.keySet() == expectedSessionIds as Set
        sessionRepository.retrieveSession(existingSessionId).get().getProcessingState().getState() == expectedExistingState

        where:
        restart | expectedLifecycle              | expectedActiveSessionId | expectedSessionIds                 | expectedExistingState
        false   | [SESSION_RESUME]               | new SessionId('EXISTING-SESSION') | [new SessionId('EXISTING-SESSION')] | State.RUNNING
        true    | [SESSION_STOP, SESSION_START]  | new SessionId('NEW-SESSION')      | [new SessionId('EXISTING-SESSION'), new SessionId('NEW-SESSION')] | State.DONE
    }

    def 'stop event with active session emits SESSION_STOP and clears active session'() {
        given:
        InstanceId instanceId = new InstanceId('0001-demo-instance')
        SessionId existingSessionId = new SessionId('EXISTING-SESSION')
        Session existingSession = runningSession(instanceId, existingSessionId)
        List<InternalEvent> historyEvents = []
        List<InternalEvent> queuedInstanceEvents = []

        InMemoryActiveSessionIdRepository activeSessionIdRepository =
            new InMemoryActiveSessionIdRepository([(instanceId): existingSessionId], [])
        InMemorySessionRepository sessionRepository =
            new InMemorySessionRepository(instanceId, [(existingSessionId): existingSession])

        InternalEventClassifier classifier = Stub() {
            isRelevant(_ as InternalEvent) >> true
            isValid(_ as InternalEvent) >> true
            getTimeout() >> Optional.empty()
            determineDynamicContent(_ as InternalEvent) >> Optional.empty()
            getCurrentRun() >> Optional.empty()
            isSessionStartEvent(_ as InternalEvent) >> false
            isSessionStopEvent(_ as InternalEvent, _ as Session) >> true
        }

        when:
        createRunnable(
            classifier,
            createTriggerEvent(),
            instanceId,
            activeSessionIdRepository,
            sessionRepository,
            historyEvents,
            queuedInstanceEvents
        ).run()

        then:
        lifecycleAttributes(historyEvents) == [SESSION_STOP]
        activeSessionIdRepository.retrieveActiveSessionId(instanceId).isEmpty()
        sessionRepository.retrieveSession(existingSessionId).get().getProcessingState().getState() == State.DONE
    }

    private InternalEventHandlerRunnable createRunnable(
        InternalEventClassifier classifier,
        InternalEvent triggerEvent,
        InstanceId instanceId,
        InMemoryActiveSessionIdRepository activeSessionIdRepository,
        InMemorySessionRepository sessionRepository,
        List<InternalEvent> historyEvents,
        List<InternalEvent> queuedInstanceEvents
    ) {
        return new InternalEventHandlerRunnable(
            Stub(InstanceRepository) {
                retrieveInstanceIds() >> [instanceId]
                retrieveInstance(instanceId) >> Optional.of(new Instance(instanceId))
            },
            Stub(SessionRepositoryFactory) {
                retrieveSessionRepository(_ as InstanceId) >> sessionRepository
            },
            activeSessionIdRepository,
            new ListEventHistory(historyEvents),
            Stub(OrphanEventSource) {
                drainOrphanEventsInto(_ as InternalEventClassifier, _ as EventAffiliation, _ as Drain<InternalEvent>) >> false
                getOrphansCount(_ as InstanceId) >> 0
            },
            Stub(InternalEventReceiver) {
                receiveEvent() >>> [Optional.of(triggerEvent), Optional.empty()]
                isEmpty() >> false
                getSize() >> 0
            },
            Stub(InstanceEventSenderResolver) {
                resolveEventSender(_ as InstanceId) >> new CollectingInternalEventSender(queuedInstanceEvents)
            },
            Stub(TimestampFactory) {
                createTimestamp() >>> [
                    LocalDateTime.of(2026, 6, 17, 10, 15, 1),
                    LocalDateTime.of(2026, 6, 17, 10, 15, 2),
                    LocalDateTime.of(2026, 6, 17, 10, 15, 3),
                    LocalDateTime.of(2026, 6, 17, 10, 15, 4)
                ]
            },
            Stub(InternalEventClassifierMappingResolver) {
                resolveMapping() >> [(instanceId): classifier]
            },
            Stub(SessionInitializerMappingResolver) {
                resolveMapping() >> [:]
            },
            new SilentEmailSenderFactory(),
            Stub(RunRepository) {
                retrieveActiveRun() >> Optional.empty()
                retrieveWaitingRun() >> Optional.empty()
                retrieveActiveRunStartTimestamp() >> Optional.empty()
                retrieveWaitingRunStartTimestamp() >> Optional.empty()
                createRun(_ as String) >> {}
                completeRun() >> {}
            },
            'test'
        )
    }

    private static InternalEvent createTriggerEvent() {
        InternalEvent triggerEvent = new InternalEvent()
        triggerEvent.setId('incoming-file')
        triggerEvent.setType(InternalEventType.FILE)
        triggerEvent.setTimestamp(LocalDateTime.of(2026, 6, 17, 10, 15))
        return triggerEvent
    }

    private static Session runningSession(InstanceId instanceId, SessionId sessionId) {
        Session session = new Session(
            instanceId,
            sessionId,
            LocalDateTime.of(2026, 6, 16, 23, 55),
            [:],
            new Session.SessionUpdates()
        )
        SessionProcessingState processingState = new SessionProcessingState(null)
        processingState.setState(State.RUNNING)
        processingState.setStarted(LocalDateTime.of(2026, 6, 16, 23, 55))
        session.setProcessingState(processingState)
        return session
    }

    private static List<String> lifecycleAttributes(List<InternalEvent> events) {
        return events
            .findAll { it.getType() == InternalEventType.SESSION && it.getId() == SESSION_LIFECYCLE }
            .collect { it.getAttributeValue(SESSION_LIFECYCLE_ATTRIBUTE_NAME).orElse(null) }
    }

    private static class CollectingInternalEventSender implements InternalEventSender {
        private final List<InternalEvent> events

        CollectingInternalEventSender(List<InternalEvent> events) {
            this.events = events
        }

        @Override
        boolean isEmpty() {
            return events.isEmpty()
        }

        @Override
        boolean isBlocking() {
            return false
        }

        @Override
        void sendEvent(InternalEvent event) {
            events.add(event)
        }
    }

    private static class ListEventHistory implements EventHistory {
        private final List<InternalEvent> events

        ListEventHistory(List<InternalEvent> events) {
            this.events = events
        }

        @Override
        void add(InternalEvent event) {
            events.add(event)
        }

        @Override
        void addAll(Iterable<InternalEvent> events) {
            events.each(this.&add)
        }
    }

    private static class InMemoryActiveSessionIdRepository implements ActiveSessionIdRepository {
        private final Map<InstanceId, SessionId> activeSessionIds
        private final Queue<SessionId> generatedSessionIds

        InMemoryActiveSessionIdRepository(Map<InstanceId, SessionId> activeSessionIds, List<SessionId> generatedSessionIds) {
            this.activeSessionIds = new LinkedHashMap<>(activeSessionIds)
            this.generatedSessionIds = new ArrayDeque<>(generatedSessionIds)
        }

        @Override
        Collection<InstanceId> listAllActiveInstances() {
            return activeSessionIds.keySet()
        }

        @Override
        Optional<SessionId> createActiveSessionId(InstanceId instanceId) {
            if (activeSessionIds.containsKey(instanceId)) {
                return Optional.empty()
            }

            SessionId sessionId = generatedSessionIds.remove()
            activeSessionIds.put(instanceId, sessionId)
            return Optional.of(sessionId)
        }

        @Override
        Optional<SessionId> retrieveActiveSessionId(InstanceId instanceId) {
            return Optional.ofNullable(activeSessionIds.get(instanceId))
        }

        @Override
        SessionId retrieveOrCreateActiveSessionId(InstanceId instanceId) {
            return retrieveActiveSessionId(instanceId).orElseGet {
                SessionId sessionId = generatedSessionIds.remove()
                activeSessionIds.put(instanceId, sessionId)
                sessionId
            }
        }

        @Override
        boolean deleteActiveSessionId(InstanceId instanceId) {
            return activeSessionIds.remove(instanceId) != null
        }
    }

    private static class InMemorySessionRepository implements SessionRepository {
        private final InstanceId instanceId
        private final Map<SessionId, Session> sessions

        InMemorySessionRepository(InstanceId instanceId, Map<SessionId, Session> sessions) {
            this.instanceId = instanceId
            this.sessions = new LinkedHashMap<>(sessions)
        }

        @Override
        Set<SessionId> retrieveSessionIds() {
            return sessions.keySet()
        }

        @Override
        Set<SessionId> retrieveSessionIds(LocalDate createdOn) {
            return sessions.findAll { it.value.getCreationTimestamp().toLocalDate() == createdOn }.keySet()
        }

        @Override
        Optional<Session> retrieveSession(SessionId sessionId) {
            return Optional.ofNullable(sessions.get(sessionId))
        }

        @Override
        Session createSession(SessionId id) {
            throw new UnsupportedOperationException()
        }

        @Override
        Session createSession(SessionId sessionId, LocalDateTime createdOn, Map<String, String> config) {
            Session session = new Session(instanceId, sessionId, createdOn, config, new Session.SessionUpdates())
            sessions.put(sessionId, session)
            return session
        }

        @Override
        Session retrieveOrCreateSession(SessionId sessionId) {
            Session session = sessions.get(sessionId)
            if (session == null) {
                session = new Session(instanceId, sessionId)
                sessions.put(sessionId, session)
            }
            return session
        }

        @Override
        Session updateSession(Session session) {
            sessions.put(session.getId(), session)
            return session
        }

        @Override
        boolean deleteSession(SessionId sessionId) {
            return sessions.remove(sessionId) != null
        }

        @Override
        Session modifySession(SessionId sessionId, java.util.function.Consumer<Session> modifier) {
            Session session = sessions.get(sessionId)
            modifier.accept(session)
            sessions.put(sessionId, session)
            return session
        }
    }

    private static class SilentEmailSenderFactory implements EmailSenderFactory {
        private static final EmailSender EMAIL_SENDER = new EmailSender() {
            @Override
            ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message) {
                return null
            }

            @Override
            ExecutorResult<ExecutorExitCode> sendEmailWithLink(String subject, String message, String linkName) {
                return null
            }

            @Override
            ExecutorResult<ExecutorExitCode> sendEmailWithLink(
                String subject,
                String message,
                String linkTarget,
                String linkName
            ) {
                return null
            }

            @Override
            ExecutorResult<ExecutorExitCode> sendEmail(String subject, String message, String parameter) {
                return null
            }

            @Override
            ExecutorResult<ExecutorExitCode> sendEmail(
                String subject,
                String message,
                String parameter,
                String filename,
                String attachmentName
            ) {
                return null
            }
        }

        @Override
        EmailSender getSender(String env, RecipientsCluster cluster, EmailType type) {
            return EMAIL_SENDER
        }

        @Override
        EmailSender getSender(String env, String recipients, EmailType type) {
            return EMAIL_SENDER
        }
    }
}
