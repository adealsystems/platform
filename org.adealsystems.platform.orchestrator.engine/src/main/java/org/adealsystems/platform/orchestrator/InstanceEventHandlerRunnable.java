/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.adealsystems.platform.orchestrator.status.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;

import static org.adealsystems.platform.orchestrator.InternalEvent.setSessionStateAttribute;
import static org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.FINAL_UNSUCCESSFUL_STATES;
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

    @Override
    public void run() {
        while (true) {
            Optional<InternalEvent> oEvent = eventReceiver.receiveEvent();
            if (!oEvent.isPresent()) {
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

            Optional<Session> oSession = sessionRepository.retrieveSession(sessionId);
            if (!oSession.isPresent()) {
                LOGGER.warn("No session available for instanceId {} and sessionId {}!", instanceId, sessionId);
                continue;
            }

            Session session = oSession.get();
            Session previousSession = session.clone();

            // Get current session processing state
            SessionProcessingState.update(session, processingState -> {
                if (processingState.getState() == State.READY_TO_RUN) {
                    processingState.setState(State.RUNNING);
                    session.setProcessingState(processingState);
                }
            });

            try {
                InternalEvent returnedEvent = instanceEventHandler.handle(event, session);
                InternalEvent processedEvent = InternalEvent.deriveProcessedInstance(returnedEvent);
                eventHistory.add(processedEvent);
            } catch (Exception ex) {
                LOGGER.error("Exception while handling event {} with session {}!", event, session, ex);
            }

            if (instanceEventHandler.isTerminating(event)) {
                LOGGER.debug("Finalizing session {}", session);
                SessionProcessingState.update(session, processingState -> {
                    processingState.setTerminated(LocalDateTime.now(ZoneId.systemDefault()));
                    SessionProcessingState.buildTerminationMessage(session, processingState);

                    if (!FINAL_UNSUCCESSFUL_STATES.contains(processingState.getState())) {
                        processingState.setState(State.DONE);
                    }
                });

                // reset terminating flag
                instanceEventHandler.resetTerminatingFlag(event);
            }

            if (session.equals(previousSession)) {
                LOGGER.debug("Session wasn't changed by event handler {}", session);
                continue;
            }

            LOGGER.debug("Updating session from {} to {}.", previousSession, session);
            sessionRepository.updateSession(session);

            InternalEvent changeSessionEvent = createSessionStateEvent(session);
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
}
