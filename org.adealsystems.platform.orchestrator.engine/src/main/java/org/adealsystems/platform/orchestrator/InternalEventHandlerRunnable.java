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

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.orchestrator.executor.email.EmailParamMapper;
import org.adealsystems.platform.orchestrator.executor.email.EmailSender;
import org.adealsystems.platform.orchestrator.executor.email.EmailSenderFactory;
import org.adealsystems.platform.orchestrator.executor.email.EmailType;
import org.adealsystems.platform.orchestrator.executor.email.RecipientsCluster;
import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.adealsystems.platform.orchestrator.status.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static org.adealsystems.platform.orchestrator.InternalEvent.ATTR_RUN_ID;
import static org.adealsystems.platform.orchestrator.InternalEvent.getDynamicContentAttribute;
import static org.adealsystems.platform.orchestrator.InternalEvent.getSessionStateAttribute;
import static org.adealsystems.platform.orchestrator.InternalEvent.getSourceEventAttribute;
import static org.adealsystems.platform.orchestrator.InternalEvent.normalizeDynamicContent;
import static org.adealsystems.platform.orchestrator.InternalEvent.setDynamicContentAttribute;
import static org.adealsystems.platform.orchestrator.InternalEvent.setMinimizedSourceEventAttribute;
import static org.adealsystems.platform.orchestrator.InternalEvent.setSessionStateAttribute;
import static org.adealsystems.platform.orchestrator.InternalEventUtilities.isFileSizeZero;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.DYNAMIC_CONTENT_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.INSTANCE_ID_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_ID_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_LIFECYCLE;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_LIFECYCLE_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_RESUME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_START;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_STATE;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_STOP;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.TERMINATING_FLAG;

public class InternalEventHandlerRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(InternalEventHandlerRunnable.class);

    public static final Set<String> INTERNAL_SESSION_STATE_IDS = Set.of(
        SESSION_START,
        SESSION_RESUME,
        SESSION_STOP
    );

    private static final Set<String> LOOP_SANITY_CHECK_IDS = Set.of(
        SESSION_LIFECYCLE
    );

    public static final Set<State> FINAL_UNSUCCESSFUL_STATES = Set.of(
        State.FAILED,
        State.CANCELLED,
        State.ABORTED
    );

    public static final Set<State> FINAL_STATES = Set.of(
        State.DONE,
        State.FAILED,
        State.CANCELLED,
        State.ABORTED
    );

    public static final String EVENT_ID_CREATE_RUN = "create-run";
    public static final String EVENT_ID_COMPLETE_RUN = "complete-run";

    private static final Pattern DYNAMIC_CONTENT_PATTERN = Pattern.compile("[0-9a-zA-Z]*([,@%\\_\\-\\.0-9a-zA-Z/]+)*");
    private static final int MAX_BLOCKING_RETRY_ATTEMPTS = 3;
    private static final int BLOCKING_RETRY_DELAY = 2 * 1_000;
    private static final int ORPHAN_ADD_DELAY = 3 * 1_000;

    private final InstanceRepository instanceRepository;
    private final SessionRepositoryFactory sessionRepositoryFactory;
    private final ActiveSessionIdRepository activeSessionIdRepository;
    private final EventHistory eventHistory;
    private final OrphanEventSource orphanEventSource;
    private final InternalEventReceiver rawEventReceiver;
    private final InstanceEventSenderResolver instanceEventSenderResolver;
    private final TimestampFactory timestampFactory;
    private final InternalEventClassifierMappingResolver eventClassifierMappingResolver;
    private final SessionInitializerMappingResolver sessionInitializerMappingResolver;
    private final RunRepository runRepository;
    private final EmailSenderFactory emailSenderFactory;
    private final String environment;

    private String currentProcessingEvent;

    @SuppressWarnings("PMD.ExcessiveParameterList")
    public InternalEventHandlerRunnable(
        InstanceRepository instanceRepository,
        SessionRepositoryFactory sessionRepositoryFactory,
        ActiveSessionIdRepository activeSessionIdRepository,
        EventHistory eventHistory,
        OrphanEventSource orphanEventSource,
        InternalEventReceiver rawEventReceiver,
        InstanceEventSenderResolver instanceEventSenderResolver,
        TimestampFactory timestampFactory,
        InternalEventClassifierMappingResolver eventClassifierMappingResolver,
        SessionInitializerMappingResolver sessionInitializerMappingResolver,
        EmailSenderFactory emailSenderFactory,
        RunRepository runRepository,
        String environment
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
        this.eventHistory = Objects.requireNonNull(
            eventHistory,
            "eventHistory must not be null!"
        );
        this.orphanEventSource = Objects.requireNonNull(
            orphanEventSource,
            "orphanEventSource must not be null!"
        );
        this.rawEventReceiver = Objects.requireNonNull(
            rawEventReceiver,
            "rawEventReceiver must not be null!"
        );
        this.instanceEventSenderResolver = Objects.requireNonNull(
            instanceEventSenderResolver,
            "instanceEventSenderResolver must not be null!"
        );
        this.timestampFactory = Objects.requireNonNull(
            timestampFactory,
            "timestampFactory must not be null!"
        );
        this.eventClassifierMappingResolver = Objects.requireNonNull(
            eventClassifierMappingResolver,
            "eventClassifierMappingResolver must not be null!"
        );
        this.sessionInitializerMappingResolver = Objects.requireNonNull(
            sessionInitializerMappingResolver,
            "sessionInitializerMappingResolver must not be null!"
        );
        this.runRepository = Objects.requireNonNull(
            runRepository,
            "runRepository must not be null!"
        );
        this.emailSenderFactory = Objects.requireNonNull(
            emailSenderFactory,
            "emailSenderFactory must not be null!"
        );
        this.environment = Objects.requireNonNull(
            environment,
            "environment must not be null!"
        );
    }

    public String getCurrentProcessingEvent() {
        return currentProcessingEvent;
    }

    protected InternalEventReceiver getRawEventReceiver() {
        return rawEventReceiver;
    }

    @Override
    public void run() {
        List<InternalEvent> currentEvents = new ArrayList<>();

        Set<InternalEvent> dejaVu = new HashSet<>();
        while (true) {
            try {
                dejaVu.clear();
                currentEvents.clear();

                LOGGER.debug("Getting a new raw event");
                currentProcessingEvent = "";
                Optional<InternalEvent> oEvent = rawEventReceiver.receiveEvent();
                if (oEvent.isEmpty()) {
                    LOGGER.info("Shutting down internal event handler thread. Raw event receiver returned no value.");
                    return;
                }

                InternalEvent event = oEvent.get();
                String eventId = event.getId();
                InternalEventType eventType = event.getType();
                currentProcessingEvent = eventId;
                LOGGER.debug("Analysing a new received event {} of type {}", eventId, eventType);

                // Session events special handling
                if (eventType == InternalEventType.SESSION && !SESSION_STATE.equals(eventId)) {
                    // Session state events shouldn't be sent to owner instance
                    LOGGER.warn("Ignoring {} event {}!", InternalEventType.SESSION, event);
                    continue;
                }

                // Run events special handling
                if (eventType == InternalEventType.RUN) {
                    Optional<String> oRunId = event.getAttributeValue(ATTR_RUN_ID);
                    if (oRunId.isEmpty()) {
                        LOGGER.warn("Invalid RUN event, missing run-id attribute! Ignoring...");
                        continue;
                    }
                    String runId = oRunId.get();

                    switch (eventId) {
                        case EVENT_ID_CREATE_RUN:
                            // cleanup all active sessions for the current active run
                            cleanupActiveRunningSessions(runId);
                            LOGGER.info("Starting a new run '{}'", runId);
                            runRepository.createRun(runId);
                            break;
                        case EVENT_ID_COMPLETE_RUN:
                            cleanupAllRunningSessions(runId);
                            LOGGER.info("Completing the run '{}'", runId);
                            runRepository.completeRun();
                            break;
                        default:
                            LOGGER.warn("Unknown/unsupported run specific event '{}'!", eventId);
                            break;
                    }
                    continue;
                }

                // File event special handling
                if (eventType == InternalEventType.FILE && isFileSizeZero(event)) {
                    LOGGER.debug("Ignoring zero byte file event {}!", event);
                    continue;
                }

                currentEvents.add(event);
                if (eventType == InternalEventType.SESSION && hasFlagForTerminateSession(event)) {
                    stopSessionFlaggedToTerminate(event, currentEvents);
                }

                LOGGER.debug("Initial checking for non-propagated events in {}", currentEvents);
                while (!currentEvents.isEmpty()) {
                    List<InternalEvent> newEvents = new ArrayList<>(); // NOPMD AvoidInstantiatingObjectsInLoops
                    for (InternalEvent currentEvent : currentEvents) {
                        newEvents.addAll(propagateEvent(currentEvent, dejaVu));
                    }
                    currentEvents = newEvents;
                    LOGGER.debug("Checking for non-propagated events in {}", currentEvents);
                }
            }
            catch (Throwable throwable) {
                LOGGER.error("Error in the main loop of InternalEventHandler!", throwable);
                try {
                    EmailSender emailSender = emailSenderFactory.getSender(
                        environment,
                        RecipientsCluster.INTERNAL,
                        EmailType.ERROR
                    );
                    emailSender.sendEmail("Error in the main loop of InternalEventHandler!", String.valueOf(throwable));
                }
                catch (Throwable th) {
                    LOGGER.error("Unable to send email!", th);
                }
            }
        }
    }

    private void cleanupActiveRunningSessions(String runId) {
        Set<String> activeSessionsInfo = cleanupRunningSessions(
            runId, (runSpecification, session) ->
                runSpecification.getType() == RunType.ACTIVE
        );

        if (activeSessionsInfo.isEmpty()) {
            LOGGER.info("No active running sessions available on start-run-event for '{}'", runId);
        }
        else {
            LOGGER.warn(
                "Following sessions were active and closed on start-run-event for '{}': {}",
                runId,
                activeSessionsInfo
            );
            EmailSender emailSender = emailSenderFactory.getSender(
                environment,
                RecipientsCluster.INTERNAL,
                EmailType.ERROR
            );
            emailSender.sendEmail(
                "Creating new RUN",
                "Following instances were active and closed on start-run-event for '" + runId + "'",
                EmailParamMapper.mapObject(activeSessionsInfo)
            );
        }
    }

    private void cleanupAllRunningSessions(String runId) {
        Set<String> activeSessionsInfo = cleanupRunningSessions(
            runId, (runSpecification, session) -> {
                Optional<String> oSessionRunId = session.getStateValue(SessionEventConstants.RUN_ID_ATTRIBUTE_NAME);
                if (oSessionRunId.isEmpty()) {
                    LOGGER.info("No run-id specified in the session {}", session);
                    return false;
                }
                return runId.equals(oSessionRunId.get());
            }
        );

        if (activeSessionsInfo.isEmpty()) {
            LOGGER.info("No active sessions available on complete-run-event for '{}'", runId);
        }
        else {
            LOGGER.warn(
                "Following sessions were active and closed on complete-run-event for '{}': {}",
                runId,
                activeSessionsInfo
            );
            EmailSender emailSender = emailSenderFactory.getSender(
                environment,
                RecipientsCluster.INTERNAL,
                EmailType.ERROR
            );
            emailSender.sendEmail(
                "Completing new RUN",
                "Following instances were active and closed on complete-run-event for '" + runId + "'",
                EmailParamMapper.mapObject(activeSessionsInfo)
            );
        }
    }

    private Set<String> cleanupRunningSessions(String runId, BiFunction<RunSpecification, Session, Boolean> verifier) {
        Collection<InstanceId> activeInstances = activeSessionIdRepository.listAllActiveInstances();
        if (activeInstances == null || activeInstances.isEmpty()) {
            LOGGER.info("No active sessions available on start-run-event for '{}'", runId);
            return Collections.emptySet();
        }

        Collection<InstanceId> allStaticIds = instanceRepository.retrieveInstanceIds();

        Map<InstanceId, InternalEventClassifier> classifierMapping = eventClassifierMappingResolver.resolveMapping();
        Set<String> activeSessionsInfo = new HashSet<>();
        for (InstanceId instanceId : activeInstances) {
            Optional<SessionId> oSessionId = activeSessionIdRepository.retrieveActiveSessionId(instanceId);
            if (oSessionId.isEmpty()) {
                LOGGER.debug("No active session found for {}", instanceId);
                continue;
            }

            // special handling for dynamic instances
            InstanceId instanceRef = resolveInstanceId(instanceId, allStaticIds);
            SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(instanceRef);

            SessionId sessionId = oSessionId.get();
            Optional<Session> oSession = sessionRepository.retrieveSession(sessionId);
            if (oSession.isEmpty()) {
                LOGGER.info("No session found for {}", sessionId);
                continue;
            }

            InternalEventClassifier classifier = classifierMapping.get(instanceRef);
            if (classifier == null) {
                LOGGER.error("No classifier found for running instance '{}'!", instanceRef);
                continue;
            }

            Optional<RunSpecification> oRun = classifier.getCurrentRun();
            if (oRun.isEmpty()) {
                LOGGER.debug("Event classifier for {} is not run-specific", instanceRef);
                continue;
            }

            RunSpecification runSpec = oRun.get();
            Session session = oSession.get();
            if (!verifier.apply(runSpec, session)) {
                LOGGER.info("Instance {} categorized as is not applicable for {} and {}", instanceId, runSpec, session);
                continue;
            }

            activeSessionsInfo.add(instanceId.getId());
            activeSessionIdRepository.deleteActiveSessionId(instanceId);
        }

        return activeSessionsInfo;
    }

    private static InstanceId resolveInstanceId(InstanceId instanceId, Collection<InstanceId> allStaticIds) {
        InstanceId instanceRef = null;
        String id = instanceId.getId();
        for (InstanceId staticId : allStaticIds) {
            if (staticId.getId().equals(id)) {
                // static
                instanceRef = instanceId;
                break;
            }
        }
        if (instanceRef == null) {
            // dynamic
            int pos = id.lastIndexOf('-');
            instanceRef = new InstanceId(id.substring(0, pos)); // NOPMD
        }

        return instanceRef;
    }

    private boolean hasFlagForTerminateSession(InternalEvent event) {
        Optional<Session> oSession = getSessionStateAttribute(event);
        if (oSession.isEmpty()) {
            return false;
        }

        Session session = oSession.get();
        return session.hasFinishedFlag() || session.hasFailedFlag() || session.hasCancelledFlag();
    }

    private List<InternalEvent> propagateEvent(InternalEvent event, Set<InternalEvent> dejaVu) {
        LOGGER.debug("Propagating event: {}", event);

        if (event.getInstanceId() != null || event.getSessionId() != null) {
            LOGGER.warn("Raw event contains instanceId or sessionId, fixing it: {}", event);
            event.setInstanceId(null);
            event.setSessionId(null);
        }

        if (dejaVu.contains(event)) {
            LOGGER.debug("Ignoring already processed event {}.", event);
            return Collections.emptyList();
        }

        dejaVu.add(event);
        eventHistory.add(event);

        return handleEvent(event);
    }

    private List<InternalEvent> handleEvent(InternalEvent event) {
        LOGGER.debug("Handling event: {}", event);

        // mapping: event -> instance
        List<InternalEvent> sessionStateEvents = new ArrayList<>();

        boolean assigned = false;
        Map<InstanceId, InternalEventClassifier> eventClassifierMapping = eventClassifierMappingResolver.resolveMapping();
        for (Map.Entry<InstanceId, InternalEventClassifier> entry : eventClassifierMapping.entrySet()) {
            InternalEventClassifier eventClassifier = entry.getValue();

            InternalEvent clonedEvent = event.clone();

            if (clonedEvent.getTimestamp() == null) {
                LOGGER.debug("Extending event with current timestamp: {}", clonedEvent);
                clonedEvent.setTimestamp(timestampFactory.createTimestamp());
            }

            // Special handling for ABORT events
            // TODO: determine referenced sessions, check if they are active and set a finished/cancelled/failed
            //  or whatever flag. Also verify, if session needs an event to close itself!

            boolean relevant = false;
            try {
                relevant = eventClassifier.isRelevant(clonedEvent);
            }
            catch (Exception ex) {
                LOGGER.error(
                    "Exception while calling isRelevant with event {} on {}!",
                    clonedEvent,
                    eventClassifier,
                    ex
                );
            }

            if (!relevant) {
                continue;
            }

            // Determine & set dynamic content, if any
            Optional<String> oDynamicContent = eventClassifier.determineDynamicContent(clonedEvent);
            if (oDynamicContent.isPresent()) {
                String content = oDynamicContent.get();
                if (!DYNAMIC_CONTENT_PATTERN.matcher(content).matches()) {
                    LOGGER.error(
                        "Dynamic content '{}' does not match pattern {}",
                        content,
                        DYNAMIC_CONTENT_PATTERN.pattern()
                    );
                    continue;
                }
                LOGGER.info("Initializing event with dynamic content {}: '{}'", clonedEvent, content);
                setDynamicContentAttribute(clonedEvent, content);
            }

            // Outdated events check (run specific)
            Optional<RunSpecification> oRun = eventClassifier.getCurrentRun();
            String classifierName = eventClassifier.getClass().getName();
            if (oRun.isPresent()) {
                RunSpecification run = oRun.get();
                LOGGER.debug("EventClassifier {} is run-specific for {}, checking validity", classifierName, run);
                if (RunSpecification.isEventOutdated(clonedEvent, run.getType(), runRepository)) {
                    LOGGER.warn("Detected an outdated event {}, current run-id: '{}'", clonedEvent, run);
                    continue;
                }

                // Validity check
                if (!eventClassifier.isValid(clonedEvent)) {
                    LOGGER.warn(
                        "Detected an invalid event {} for current run-id '{}' and {}",
                        clonedEvent,
                        run,
                        classifierName
                    );
                    continue;
                }

                clonedEvent.setAttributeValue(ATTR_RUN_ID, run.getId());
            }
            else {
                LOGGER.debug("Event {} is not run-specific, checking validity", clonedEvent);

                // Validity check
                if (!eventClassifier.isValid(clonedEvent)) {
                    LOGGER.warn("Detected an invalid event {} for {}", clonedEvent, classifierName);
                    continue;
                }
            }

            // Found a corresponding instance
            InstanceId instanceId = entry.getKey();
            clonedEvent.setInstanceId(instanceId);
            assigned = true;

            InstanceId finalInstanceId = resolveDynamicInstanceId(clonedEvent);
            LOGGER.debug("Resolved final InstanceId {}", finalInstanceId);

            // Special handling for CANCEL events
            // check if session is active, then put the event to the instance queue, otherwise ignore it
            Optional<SessionId> oSessionId = activeSessionIdRepository.retrieveActiveSessionId(finalInstanceId);
            if (clonedEvent.getType() == InternalEventType.CANCEL && oSessionId.isEmpty()) {
                LOGGER.debug(
                    "Ignoring CANCEL event {} for a not active instance {} to avoid it in the orphan queue.",
                    clonedEvent,
                    finalInstanceId
                );
                continue;
            }

            String eventId = clonedEvent.getId();
            String eventInstanceId = clonedEvent.getAttributeValue(INSTANCE_ID_ATTRIBUTE_NAME).orElse(null);
            if (InternalEventType.SESSION == clonedEvent.getType()
                && LOOP_SANITY_CHECK_IDS.contains(eventId)
                && instanceId.getId().equals(eventInstanceId)
            ) {
                LOGGER.debug("Ignoring event {} for own instance {} to prevent loop.", clonedEvent, instanceId);
                continue;
            }

            boolean isStartEvent;
            try {
                isStartEvent = eventClassifier.isSessionStartEvent(clonedEvent);
            }
            catch (Exception ex) {
                isStartEvent = false;
                LOGGER.error(
                    "Exception while calling isSessionStartEvent with event {} on {}!",
                    clonedEvent,
                    eventClassifier,
                    ex
                );
            }

            if (isStartEvent) {
                LOGGER.debug("Starting session");
                Optional<InternalEvent> optionalEvent = startSession(eventClassifier, clonedEvent);
                if (optionalEvent.isPresent()) {
                    optionalEvent = deriveSessionStateEvent(optionalEvent.get());
                    optionalEvent.ifPresent(sessionStateEvents::add);
                }
            }

            boolean isStopEvent;
            try {
                // reload the current session state (just to be sure...)
                oSessionId = activeSessionIdRepository.retrieveActiveSessionId(finalInstanceId);
                if (oSessionId.isEmpty()) {
                    isStopEvent = false;
                    LOGGER.debug(
                        "Skipping check for stop-session event, because no session is active now: {}",
                        clonedEvent
                    );
                }
                else {
                    SessionId sessionId = oSessionId.get();
                    SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(instanceId);
                    Session session = sessionRepository.retrieveSession(sessionId)
                        .orElseThrow(IllegalStateException::new);
                    isStopEvent = eventClassifier.isSessionStopEvent(clonedEvent, session);
                }
            }
            catch (Exception ex) {
                isStopEvent = false;
                LOGGER.error(
                    "Exception while calling isSessionStopEvent with event {} on {}!",
                    clonedEvent,
                    eventClassifier,
                    ex
                );
            }

            if (isStartEvent && isStopEvent) {
                isStopEvent = false;
                LOGGER.error(
                    "Event {} is considered both start event and stop event by {}!",
                    clonedEvent,
                    eventClassifier
                );
            }

            if (isStopEvent) {
                LOGGER.debug("Stopping a session");
                Optional<InternalEvent> optionalEvent = stopSession(clonedEvent);
                if (optionalEvent.isPresent()) {
                    optionalEvent = deriveSessionStateEvent(optionalEvent.get());
                    optionalEvent.ifPresent(sessionStateEvents::add);
                }

                continue;
            }

            if (oSessionId.isPresent()) {
                SessionId sessionId = oSessionId.get();
                LOGGER.debug("Set session id {} for event {}", sessionId, clonedEvent);
                clonedEvent.setSessionId(sessionId);
            }
            else {
                LOGGER.debug("Received event without active session for instance {}: {}", finalInstanceId, clonedEvent);
                if (clonedEvent.getSessionId() != null) {
                    clonedEvent.setSessionId(null);
                    LOGGER.warn("Corrected session id of event: {}", clonedEvent);
                }
            }

            LOGGER.debug("Registering instance event {}", clonedEvent);
            registerInstanceEvent(
                clonedEvent,
                instanceEventSenderResolver,
                eventHistory
            );
        }

        if (!assigned) {
            LOGGER.debug("Event could not be assigned to any event classifier: {}", event);
        }

        return sessionStateEvents;
    }

    private static Optional<InternalEvent> deriveSessionStateEvent(InternalEvent event) {
        if (InternalEventType.SESSION != event.getType()) {
            // this would be a bug
            LOGGER.error("Unexpected event {}!", event);
            return Optional.empty();
        }

        InstanceId instanceId = event.getInstanceId();
        if (instanceId == null) {
            // this would be a bug
            LOGGER.error("Missing instanceId in event {}!", event);
            return Optional.empty();
        }

        SessionId sessionId = event.getSessionId();
        if (sessionId == null) {
            // this would be a bug
            LOGGER.error("Missing sessionId in event {}!", event);
            return Optional.empty();
        }

        String id = event.getId();
        if (id == null) {
            // this would be a bug
            LOGGER.error("Missing id in event {}!", event);
            return Optional.empty();
        }

        if (!INTERNAL_SESSION_STATE_IDS.contains(id)) {
            // this would be a bug
            LOGGER.error("Unsupported id '{}' in event {}!", id, event);
            return Optional.empty();
        }

        InternalEvent resultEvent = new InternalEvent();
        resultEvent.setType(InternalEventType.SESSION);
        resultEvent.setId(SESSION_LIFECYCLE);
        resultEvent.setTimestamp(event.getTimestamp());
        resultEvent.setAttributeValue(SESSION_LIFECYCLE_ATTRIBUTE_NAME, id);
        resultEvent.setAttributeValue(INSTANCE_ID_ATTRIBUTE_NAME, instanceId.getId());
        resultEvent.setAttributeValue(SESSION_ID_ATTRIBUTE_NAME, sessionId.getId());

        // SOURCE_EVENT_ATTRIBUTE_NAME
        getSourceEventAttribute(event)
            .ifPresent(it -> setMinimizedSourceEventAttribute(resultEvent, it));
        getSessionStateAttribute(event)
            .ifPresent(it -> setSessionStateAttribute(resultEvent, it));

        LOGGER.debug("Returning derived state event {} for input event {}.", resultEvent, event);
        return Optional.of(resultEvent);
    }

    public static void registerInstanceEvent(
        InternalEvent event,
        InstanceEventSenderResolver instanceEventSenderResolver,
        EventHistory eventHistory
    ) {
        if (event.getSessionId() != null) {
            // only put event into queue if session is active
            InstanceId instanceId = event.getInstanceId();
            // InstanceId dynamicId = resolveDynamicInstanceId(event);
            InternalEventSender eventSender = instanceEventSenderResolver.resolveEventSender(instanceId);
            if (eventSender == null) {
                LOGGER.error("No event sender found for instance {}!", instanceId);
                return;
            }

            boolean added = false;
            for (int i = 0; i < MAX_BLOCKING_RETRY_ATTEMPTS; i++) {
                if (!eventSender.isBlocking()) {
                    eventSender.sendEvent(event);
                    added = true;
                    break;
                }

                LOGGER.warn(
                    "Unable to add an event, because the sender is in blocking state! Retrying after {} ms",
                    BLOCKING_RETRY_DELAY
                );
                try {
                    Thread.sleep(BLOCKING_RETRY_DELAY);
                }
                catch (InterruptedException ex) {
                    break;
                }
            }

            if (!added) {
                throw new IllegalStateException("Unable to add an event, because the sender is in blocking state!");
            }
        }
        eventHistory.add(event);
    }

    private InstanceId resolveDynamicInstanceId(InternalEvent event) {
        InstanceId instanceId = event.getInstanceId();
        Optional<String> oDynamicContent = getDynamicContentAttribute(event);
        return oDynamicContent
            .map(dynamicContent -> new InstanceId(instanceId.getId()
                                                      + '-'
                                                      + normalizeDynamicContent(dynamicContent.toLowerCase(Locale.ROOT))))
            .orElse(instanceId);
    }

    private Optional<InternalEvent> startSession(InternalEventClassifier eventClassifier, InternalEvent triggerEvent) {
        InstanceId instanceId = triggerEvent.getInstanceId();

        Optional<Instance> oInstance = instanceRepository.retrieveInstance(instanceId);
        if (oInstance.isEmpty()) {
            LOGGER.error("No instance found for id {}!", instanceId);
            return Optional.empty();
        }

        Instance instance = oInstance.get();
        InstanceId dynamicId = resolveDynamicInstanceId(triggerEvent);
        Optional<SessionId> oSessionId = activeSessionIdRepository.createActiveSessionId(dynamicId);
        boolean fresh = oSessionId.isPresent();
        SessionId sessionId;
        if (fresh) {
            sessionId = oSessionId.get();
            LOGGER.info("Created new active session {} for instance {}", sessionId, dynamicId);
        }
        else {
            sessionId = activeSessionIdRepository.retrieveOrCreateActiveSessionId(dynamicId);
            LOGGER.info("Resuming active session {} for instance {}!", sessionId, dynamicId);
        }

        SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(instanceId);

        // don't use modifySession() here. It's not really possible to have a conflict
        // at this point, because the session is new and can't be referenced from another thread.
        Session session = sessionRepository.retrieveOrCreateSession(sessionId);
        LOGGER.info("Session loaded for instance {}!", instanceId);

        Map<String, String> instanceConfiguration = instance.getConfiguration();
        if (instanceConfiguration != null && !instanceConfiguration.isEmpty()) {
            Map<String, String> state = session.getState();
            SessionProcessingState processingState = session.getProcessingState();
            session = new Session(
                session.getInstanceId(),
                session.getId(),
                session.getCreationTimestamp(),
                instanceConfiguration,
                session.getSessionUpdates()
            );
            session.setState(state);
            session.setProcessingState(processingState);
        }

        SessionProcessingState processingState = session.getProcessingState();
        if (processingState == null) {
            // create & initialize session processing state
            processingState = new SessionProcessingState(eventClassifier.getCurrentRun().orElse(null));
            processingState.setState(State.READY_TO_RUN);
            processingState.setStarted(LocalDateTime.now(ZoneId.systemDefault()));
            processingState.setConfiguration(instanceConfiguration);
            session.setProcessingState(processingState);
        }

        if (fresh) {
            LOGGER.debug("Comparing {} and {}", dynamicId, instanceId);
            if (!dynamicId.equals(instanceId)) {
                Optional<String> oDynamicContent = getDynamicContentAttribute(triggerEvent);
                if (oDynamicContent.isPresent()) {
                    LOGGER.debug("Initializing a new dynamic session for {}", dynamicId);
                    session.setStateValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME, oDynamicContent.get());
                }
            }

            Map<InstanceId, SessionInitializer> sessionInitializerMapping = sessionInitializerMappingResolver.resolveMapping();
            SessionInitializer sessionInitializer = sessionInitializerMapping.get(instanceId);
            if (sessionInitializer != null) {
                LOGGER.debug("Initializing a new session with {}", sessionInitializer);
                sessionInitializer.initializeSession(session);
                LOGGER.debug("Initialized session {}, processingState: {}", session, session.getProcessingState());
                if (session.hasFailedFlag()) {
                    LOGGER.info("Session has a failed flag after initialization: {}", session);
                    terminateSessionProcessingState(session);
                }
                else if (session.hasFinishedFlag()) {
                    LOGGER.info("Session has a finished flag after initialization: {}", session);
                    terminateSessionProcessingState(session);
                }
            }
            else {
                LOGGER.debug("No special session initializer found for instance {}", instanceId);
            }
        }

        // Add RUN-ID attribute with the corresponding value to the session, if the owner event classifier is run specific
        Optional<RunSpecification> oRun = eventClassifier.getCurrentRun();
        if (oRun.isPresent()) {
            String runId = oRun.get().getId();
            LOGGER.debug("Extending a new session with RUN-ID attribute '{}'", runId);
            session.setStateValue(SessionEventConstants.RUN_ID_ATTRIBUTE_NAME, runId);
        }

        LOGGER.debug("Creating a SESSION_START session event");
        InternalEvent startSessionEvent = new InternalEvent();
        startSessionEvent.setType(InternalEventType.SESSION);
        startSessionEvent.setId(fresh ? SESSION_START : SESSION_RESUME);
        startSessionEvent.setInstanceId(instanceId);
        startSessionEvent.setSessionId(sessionId);
        startSessionEvent.setTimestamp(timestampFactory.createTimestamp());

        setMinimizedSourceEventAttribute(startSessionEvent, triggerEvent);

        setSessionStateAttribute(startSessionEvent, session);

        LOGGER.debug("Updating a session");
        sessionRepository.updateSession(session);

        LOGGER.debug("Resolving session's dynamic content from {}", triggerEvent);
        Optional<String> dynamicContent = getDynamicContentAttribute(triggerEvent);
        dynamicContent.ifPresent(content -> setDynamicContentAttribute(startSessionEvent, content));

        LOGGER.debug("Registering start session event {}", startSessionEvent);
        registerInstanceEvent(
            startSessionEvent,
            instanceEventSenderResolver,
            eventHistory
        );

        // drain orphan events
        EventAffiliation eventAffiliation = new EventAffiliation(dynamicId, sessionId);
        LOGGER.debug("Draining orphan events of {}", eventAffiliation);
        orphanEventSource.drainOrphanEventsInto(
            eventClassifier,
            eventAffiliation,
            new OrphanEventDrain(eventAffiliation)
        );

        LOGGER.debug("Returning start session event {}", startSessionEvent);
        return Optional.of(startSessionEvent);
    }

    private Optional<InternalEvent> stopSession(InternalEvent triggerEvent) {
        InstanceId dynamicId = resolveDynamicInstanceId(triggerEvent);

        Optional<SessionId> oSessionId = activeSessionIdRepository.retrieveActiveSessionId(dynamicId);
        if (oSessionId.isEmpty()) {
            LOGGER.info("Unable to stop session! Missing active session for instance '{}'!", dynamicId);
            return Optional.empty();
        }

        SessionId sessionId = oSessionId.get();

        InstanceId instanceId = triggerEvent.getInstanceId();
        SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(instanceId);

        triggerEvent.setSessionId(sessionId);
        registerInstanceEvent(
            triggerEvent,
            instanceEventSenderResolver,
            eventHistory
        );

        InternalEvent stopSessionEvent = new InternalEvent();
        stopSessionEvent.setType(InternalEventType.SESSION);
        stopSessionEvent.setId(SESSION_STOP);
        stopSessionEvent.setInstanceId(instanceId);
        stopSessionEvent.setSessionId(sessionId);
        stopSessionEvent.setTimestamp(timestampFactory.createTimestamp());

        Optional<String> dynamicContent = getDynamicContentAttribute(triggerEvent);
        dynamicContent.ifPresent(content -> setDynamicContentAttribute(stopSessionEvent, content));

        setMinimizedSourceEventAttribute(stopSessionEvent, triggerEvent);

        Session session = sessionRepository.modifySession(
            sessionId,
            InternalEventHandlerRunnable::terminateSessionProcessingState
        );
        LOGGER.info("Session stopped: {}", session);

        setSessionStateAttribute(stopSessionEvent, session);
        registerInstanceEvent(
            stopSessionEvent,
            instanceEventSenderResolver,
            eventHistory
        );

        if (!activeSessionIdRepository.deleteActiveSessionId(dynamicId)) {
            LOGGER.error("Unable to delete active session for instance {}!", dynamicId);
        }

        return Optional.of(stopSessionEvent);
    }

    private void stopSessionFlaggedToTerminate(InternalEvent event, List<InternalEvent> currentEvents) {
        InternalEvent sessionEvent = event.clone();

        currentEvents.clear();

        Optional<String> oInstanceId = sessionEvent.getAttributeValue(INSTANCE_ID_ATTRIBUTE_NAME);
        Optional<String> oSessionId = sessionEvent.getAttributeValue(SESSION_ID_ATTRIBUTE_NAME);
        if (oInstanceId.isEmpty() || oSessionId.isEmpty()) {
            LOGGER.error(
                "Unable to terminate session, because session event does not contain attributes with instanceId or/and sessionId: {}",
                sessionEvent
            );
            return;
        }

        InstanceId instanceId = new InstanceId(oInstanceId.get());
        SessionId sessionId = new SessionId(oSessionId.get());

        SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(instanceId);
        Session session = sessionRepository.retrieveSession(sessionId)
            .orElseThrow(() -> new IllegalArgumentException(
                "Invalid session reference, no session for id '" + sessionId + "' found!")
            );

        if (session.getStateFlag(TERMINATING_FLAG)) {
            LOGGER.debug("Session {} is in a terminating state, ignoring an additional termination call", sessionId);
            return;
        }

        LOGGER.info(
            "Session {} will be closed due to one of termination flags, base event: {}",
            sessionId,
            sessionEvent
        );
        session = sessionRepository.modifySession(
            sessionId,
            s -> s.setStateFlag(TERMINATING_FLAG, true)
        );

        sessionEvent.setInstanceId(session.getInstanceId());
        sessionEvent.setSessionId(session.getId());

        Optional<String> oDynamicContent = session.getStateValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME);
        oDynamicContent.ifPresent(content -> setDynamicContentAttribute(sessionEvent, content));

        Optional<InternalEvent> oStopSessionEvent = stopSession(sessionEvent);
        if (oStopSessionEvent.isEmpty()) {
            LOGGER.info("Failed to stop session {}, event {}", sessionId, event);
            return;
        }

        LOGGER.info("Stop session {} with event {}", sessionId, sessionEvent);
        InternalEvent stopSessionEvent = oStopSessionEvent.get();
        Optional<InternalEvent> stopEvent = deriveSessionStateEvent(stopSessionEvent);
        stopEvent.ifPresent(currentEvents::add);
    }

    public static void terminateSessionProcessingState(Session session, State finalState) {
        Session.updateProcessingState(
            session, processingState -> {
                LOGGER.debug("Modifying session's processing state for session {} to {}", session.getId(), finalState);
                processingState.setTerminated(LocalDateTime.now(ZoneId.systemDefault()));
                SessionProcessingState.buildTerminationMessage(session, processingState);

                if (!FINAL_UNSUCCESSFUL_STATES.contains(processingState.getState())) {
                    processingState.setState(finalState);
                }

                session.setProcessingState(processingState);
            }
        );
    }

    public static void terminateSessionProcessingState(Session session) {
        terminateSessionProcessingState(session, State.DONE);
    }

    class OrphanEventDrain implements Drain<InternalEvent> {

        private final EventAffiliation idTuple;

        OrphanEventDrain(EventAffiliation idTuple) {
            this.idTuple = Objects.requireNonNull(idTuple, "idTuple must not be null!");
        }

        @Override
        public void add(InternalEvent event) {
            Objects.requireNonNull(event, "event must not be null!");
            //TODO
            //event.setInstanceId(idTuple.getInstanceId());
            event.setSessionId(idTuple.getSessionId());
            registerInstanceEvent(
                event,
                instanceEventSenderResolver,
                eventHistory
            );
        }

        @Override
        public void addAll(Iterable<InternalEvent> iterable) {
            Objects.requireNonNull(iterable, "iterable must not be null!");

            for (InternalEvent event : iterable) {
                if (event == null) {
                    continue;
                }

                add(event);

                // add a short break after each event
                try {
                    Thread.sleep(ORPHAN_ADD_DELAY);
                }
                catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void close() {
            // empty
        }
    }

    private static class BooleanResultContainer {
        private boolean value;

        public void setValue(boolean value) {
            this.value = value;
        }

        public boolean getValue() { // NOPMD
            return value;
        }
    }
}
