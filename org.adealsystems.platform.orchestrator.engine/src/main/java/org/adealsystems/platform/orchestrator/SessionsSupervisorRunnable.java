/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

import org.adealsystems.platform.orchestrator.status.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.adealsystems.platform.orchestrator.InternalEvent.setDynamicContentAttribute;
import static org.adealsystems.platform.orchestrator.InternalEvent.setSessionStateAttribute;
import static org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.registerInstanceEvent;
import static org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.terminateSessionProcessingState;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_STOP;

public class SessionsSupervisorRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionsSupervisorRunnable.class);

    private static final long LONG_SLEEP_INTERVAL = 5 * 60 * 1_000;
    private static final long SHORT_SLEEP_INTERVAL = 60 * 1_000;

    private final InstanceRepository instanceRepository;
    private final SessionRepositoryFactory sessionRepositoryFactory;
    private final ActiveSessionIdRepository activeSessionIdRepository;
    private final InstanceEventSenderResolver instanceEventSenderResolver;
    private final InternalEventClassifierMappingResolver eventClassifierMappingResolver;
    private final TimestampFactory timestampFactory;
    private final EventHistory eventHistory;

    private final Map<String, LocalDateTime> startSessionTimestamps = new HashMap<>();
    private final Map<String, Long> timeouts = new HashMap<>();

    public SessionsSupervisorRunnable(
        InstanceRepository instanceRepository,
        SessionRepositoryFactory sessionRepositoryFactory,
        ActiveSessionIdRepository activeSessionIdRepository,
        InstanceEventSenderResolver instanceEventSenderResolver,
        InternalEventClassifierMappingResolver eventClassifierMappingResolver,
        TimestampFactory timestampFactory,
        EventHistory eventHistory
    ) {
        this.instanceRepository = Objects.requireNonNull(
            instanceRepository,
            "instanceRepository must not be null!"
        );
        this.sessionRepositoryFactory = Objects.requireNonNull(
            sessionRepositoryFactory,
            "sessionRepositoryFactory must not be null!"
        );
        this.activeSessionIdRepository = Objects.requireNonNull(
            activeSessionIdRepository,
            "activeSessionIdRepository must not be null!"
        );
        this.instanceEventSenderResolver = Objects.requireNonNull(
            instanceEventSenderResolver,
            "instanceEventSenderResolver must not be null!"
        );
        this.eventClassifierMappingResolver = Objects.requireNonNull(
            eventClassifierMappingResolver,
            "eventClassifierMappingResolver must not be null!"
        );
        this.timestampFactory = Objects.requireNonNull(
            timestampFactory,
            "timestampFactory must not be null!"
        );
        this.eventHistory = Objects.requireNonNull(
            eventHistory,
            "eventHistory must not be null!"
        );

        initialize();
    }

    @Override
    public void run() {
        // should be started as daemon thread

        Collection<InstanceId> allStaticIds = instanceRepository.retrieveInstanceIds();

        long sleepInterval = SHORT_SLEEP_INTERVAL;
        while (true) {
            try {
                sleep(sleepInterval);

                Collection<InstanceId> activeInstances = activeSessionIdRepository.listAllActiveInstances();
                if (activeInstances == null || activeInstances.isEmpty()) {
                    LOGGER.debug("No active sessions available at all, make a longer sleep");
                    sleepInterval = LONG_SLEEP_INTERVAL;
                    continue;
                }

                LocalDateTime now = timestampFactory.createTimestamp();
                LOGGER.debug("Searching for timed out active sessions");

                sleepInterval = SHORT_SLEEP_INTERVAL;
                for (InstanceId instanceId : activeInstances) {
                    Optional<SessionId> oSessionId = activeSessionIdRepository.retrieveActiveSessionId(instanceId);
                    if (!oSessionId.isPresent()) {
                        LOGGER.debug("No active session found for {}, strange...", instanceId);
                        continue;
                    }

                    InstanceReference instanceRef = resolveInstanceReference(instanceId, allStaticIds);
                    String baseId = instanceRef.base.getId();
                    Long timeout = timeouts.get(baseId);
                    if (timeout == null) {
                        LOGGER.debug("No timeout specified for {}", baseId);
                        continue;
                    }

                    String ref = instanceId.getId();
                    LocalDateTime started = startSessionTimestamps.get(ref);
                    if (started == null) {
                        // active-session without start timestamp, set now
                        LOGGER.debug("Initializing session start event for '{}'", ref);
                        startSessionTimestamps.put(ref, now);
                        continue;
                    }

                    Duration running = Duration.between(started, now);
                    long age = running.get(ChronoUnit.MINUTES);
                    if (age < timeout) {
                        LOGGER.debug("Session's age is {} minutes, not timed out", age);
                        continue;
                    }

                    LOGGER.info(
                        "Timeout reached for instance {} after {} minutes (session's age: {} minutes)",
                        instanceId,
                        timeout,
                        age
                    );

                    stopSession(instanceRef, oSessionId.get());
                    startSessionTimestamps.remove(ref);
                }
            }
            catch (InterruptedException ex) {
                LOGGER.info("Interrupting thread!", ex);
                break;
            }
            catch (Throwable ex) {
                LOGGER.error("Unexpected error occurred", ex);
            }
        }
    }

    private void initialize() {
        Map<InstanceId, InternalEventClassifier> map = eventClassifierMappingResolver.resolveMapping();
        for (Map.Entry<InstanceId, InternalEventClassifier> entry : map.entrySet()) {
            InstanceId instanceId = entry.getKey();
            InternalEventClassifier eventClassifier = entry.getValue();
            eventClassifier.getTimeout().ifPresent(value -> {
                LOGGER.debug("Setting timeout for instance {} to {}", instanceId, value);
                this.timeouts.put(instanceId.getId(), value);
            });
        }

        LOGGER.info("Initialized timeout mapping: {}", timeouts);
    }

    private void sleep(long value) throws InterruptedException {
        Thread.sleep(value);
    }

    private static InstanceReference resolveInstanceReference(InstanceId instanceId, Collection<InstanceId> allIds) {
        InstanceId instanceRef = null;
        String id = instanceId.getId();
        for (InstanceId staticId : allIds) {
            if (staticId.getId().equals(id)) {
                // static
                instanceRef = instanceId;
                break;
            }
        }

        String dynamicContent = null;
        if (instanceRef == null) {
            // dynamic
            int pos = id.lastIndexOf('-');
            instanceRef = new InstanceId(id.substring(0, pos));
            dynamicContent = id.substring(pos + 1);
        }

        return new InstanceReference(instanceId, instanceRef, dynamicContent);
    }

    private void stopSession(InstanceReference instanceRef, SessionId sessionId) {
        LOGGER.debug("Stopping session {} of instance {}", sessionId, instanceRef);

        InstanceId base = instanceRef.base;
        SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(base);
        Optional<Session> oSession = sessionRepository.retrieveSession(sessionId);
        if (!oSession.isPresent()) {
            LOGGER.info("Missing session {} for instance {}!", sessionId, base);
            return;
        }

        InternalEvent stopSessionEvent = new InternalEvent();
        stopSessionEvent.setType(InternalEventType.SESSION);
        stopSessionEvent.setId(SESSION_STOP);
        stopSessionEvent.setInstanceId(base);
        stopSessionEvent.setSessionId(sessionId);
        stopSessionEvent.setTimestamp(timestampFactory.createTimestamp());
        stopSessionEvent.setAttributeValue("timed-out", "true");

        Session session = oSession.get();
        setSessionStateAttribute(stopSessionEvent, session);

        if (instanceRef.dynamicContent != null) {
            setDynamicContentAttribute(stopSessionEvent, instanceRef.dynamicContent);
        }

        registerInstanceEvent(
            stopSessionEvent,
            instanceEventSenderResolver,
            eventHistory
        );

        if (!activeSessionIdRepository.deleteActiveSessionId(instanceRef.current)) {
            LOGGER.error("Unable to delete active session for instance {}!", instanceRef.current);
        }

        terminateSessionProcessingState(session, State.ABORTED);
        sessionRepository.updateSession(session);
    }

    static class InstanceReference {
        InstanceId current;
        InstanceId base;
        String dynamicContent;

        public InstanceReference(InstanceId current, InstanceId base, String dynamicContent) {
            this.current = current;
            this.base = base;
            this.dynamicContent = dynamicContent;
        }
    }
}
