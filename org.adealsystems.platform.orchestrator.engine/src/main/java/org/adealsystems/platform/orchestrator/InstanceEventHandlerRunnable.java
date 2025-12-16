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

package org.adealsystems.platform.orchestrator;

import org.adealsystems.platform.orchestrator.session.SessionTimestamp;
import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.adealsystems.platform.orchestrator.status.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.adealsystems.platform.orchestrator.InternalEvent.setSessionStateAttribute;
import static org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.FINAL_UNSUCCESSFUL_STATES;
import static org.adealsystems.platform.orchestrator.Session.REG_DEPENDENCIES;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.DYNAMIC_CONTENT_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.INSTANCE_ID_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_ID_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_STATE;

public class InstanceEventHandlerRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceEventHandlerRunnable.class);

    private final InstanceId instanceId;
    private final InternalEventSender rawEventSender;
    private final InstanceEventHandler instanceEventHandler;
    private final SessionRepository sessionRepository;
    private final InternalEventReceiver eventReceiver;
    private final TimestampFactory timestampFactory;
    private final EventHistory eventHistory;

    public InstanceEventHandlerRunnable(
        InstanceId instanceId,
        InternalEventSender rawEventSender,
        InstanceEventHandler instanceEventHandler,
        SessionRepository sessionRepository,
        InternalEventReceiver eventReceiver,
        TimestampFactory timestampFactory,
        EventHistory eventHistory
    ) {
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId must not be null!");
        this.instanceEventHandler = Objects.requireNonNull(instanceEventHandler, "instanceEventHandler must not be null!");
        this.sessionRepository = Objects.requireNonNull(sessionRepository, "sessionRepository must not be null!");
        this.eventReceiver = Objects.requireNonNull(eventReceiver, "eventReceiver must not be null!");
        this.rawEventSender = Objects.requireNonNull(rawEventSender, "rawEventSender must not be null!");
        this.timestampFactory = Objects.requireNonNull(timestampFactory, "timestampFactory must not be null!");
        this.eventHistory = Objects.requireNonNull(eventHistory, "eventHistory must not be null!");
    }

    protected InternalEventReceiver getEventReceiver() {
        return eventReceiver;
    }

    @Override
    public void run() {
        while (true) {
            LOGGER.debug("Waiting for the next available event for {}", instanceId);
            Optional<InternalEvent> oEvent = eventReceiver.receiveEvent();
            if (oEvent.isEmpty()) {
                LOGGER.info("Shutting down.");
                return;
            }

            InternalEvent event = oEvent.get();
            if (!instanceId.equals(event.getInstanceId())) {
                LOGGER.warn("Missing or invalid instanceId for '{}' in event {}! Ignoring...", instanceId, event);
                continue;
            }

            SessionId sessionId = event.getSessionId();
            if (sessionId == null) {
                LOGGER.warn("No sessionId found in event {}! Ignoring...", event);
                continue;
            }

            LOGGER.debug("Processing the event {} for {}", event, instanceId);

            Optional<Session> oSession = sessionRepository.retrieveSession(sessionId);
            if (oSession.isEmpty()) {
                LOGGER.warn("No session available for instanceId {} and sessionId {}!", instanceId, sessionId);
                continue;
            }

            Session session = oSession.get();

            State currentState = session.getProcessingState().getState();
            Set<String> deps = session.getStateRegistry(REG_DEPENDENCIES);
            switch (currentState) {
                case READY_TO_RUN:
                    // initialize session's state
                    session.updateState(deps.isEmpty() ? State.RUNNING : State.WAITING_FOR_DEPENDENCIES);
                    break;

                case WAITING_FOR_DEPENDENCIES:
                    if (deps.isEmpty()) {
                        session.updateState(State.RUNNING);
                    }
                    break;

                default:
                    // nothing
            }

            try {
                // Special handling for MINUTE TIMER events
                if (event.getType() == InternalEventType.TIMER) {
                    Map<String, LocalDateTime> timers = session.getActiveTimers();
                    if (!timers.isEmpty()) {
                        LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault()).minusMinutes(1);
                        for (Map.Entry<String, LocalDateTime> entry : timers.entrySet()) {
                            String key = entry.getKey();
                            LocalDateTime timer = entry.getValue();
                            if (now.isAfter(timer)) {
                                // trigger a timer event
                                InternalEvent timerEvent = createTimerEvent(key, session);
                                rawEventSender.sendEvent(timerEvent);

                                // remove the triggered timer
                                session.removeTimer(key);
                            }
                        }
                    }
                }

                if (instanceEventHandler.isRelevant(event)) {
                    LOGGER.debug("Handling event {} with {} (session: {})", event, instanceId, sessionId);
                    InternalEvent returnedEvent = instanceEventHandler.handle(event, session);
                    LOGGER.debug("Session of {} after handling event {}: {}", instanceId, event, session);
                    InternalEvent processedEvent = InternalEvent.deriveProcessedInstance(returnedEvent);
                    eventHistory.add(processedEvent);
                }
            }
            catch (Exception ex) {
                LOGGER.error("Exception while handling event {} with session {}!", event, session, ex);
            }

            if (instanceEventHandler.isTerminating(event)) {
                LOGGER.debug("Finalizing session {}", session);
                session.updateTimestamp(SessionTimestamp.TERMINATED, LocalDateTime.now(ZoneId.systemDefault()));
                session.updateMessage(SessionProcessingState.buildTerminationMessage(session));

                SessionProcessingState state = session.getProcessingState();
                if (!FINAL_UNSUCCESSFUL_STATES.contains(state.getState())) {
                    session.updateState(State.DONE);
                }

                // reset terminating flag
                instanceEventHandler.resetTerminatingFlag(event);
            }

            LOGGER.debug("Updating session to {}.", session);
            Session updatedSession = sessionRepository.updateSession(session);

            InternalEvent changeSessionEvent = createSessionStateEvent(updatedSession);
            LOGGER.debug("Sending change session event {} to handler for {}", event, instanceId);
            rawEventSender.sendEvent(changeSessionEvent);
        }
    }

    private InternalEvent createSessionStateEvent(Session session) {
        InternalEvent result = new InternalEvent();
        result.setType(InternalEventType.SESSION);
        result.setId(SESSION_STATE);
        result.setTimestamp(timestampFactory.createTimestamp());
        result.setAttributeValue(INSTANCE_ID_ATTRIBUTE_NAME, session.getInstanceId().getId());
        result.setAttributeValue(SESSION_ID_ATTRIBUTE_NAME, session.getId().getId());

        setSessionStateAttribute(result, session);

        return result;
    }

    private InternalEvent createTimerEvent(String timerId, Session session) {
        InternalEvent result = new InternalEvent();

        result.setType(InternalEventType.TIMER);
        result.setTimestamp(timestampFactory.createTimestamp());
        result.setId(timerId);

        Optional<String> dynamicContent = session.getStateValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME);
        dynamicContent.ifPresent(s -> result.setAttributeValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME, s));

        return result;
    }
}
