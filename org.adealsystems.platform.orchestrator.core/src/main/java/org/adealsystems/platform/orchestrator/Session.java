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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.adealsystems.platform.orchestrator.session.SessionSetProgressMaxValueOperation;
import org.adealsystems.platform.orchestrator.session.SessionUpdateFailedProgressOperation;
import org.adealsystems.platform.orchestrator.session.SessionUpdateProcessingStateOperation;
import org.adealsystems.platform.orchestrator.session.SessionUpdateProgressOperation;
import org.adealsystems.platform.orchestrator.session.SessionUpdateStateValueOperation;
import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Session implements Cloneable, Serializable {
    private static final long serialVersionUID = -4977538740085095596L;

    private static final Logger LOGGER = LoggerFactory.getLogger(Session.class);

    public static final String FLAG_ERROR_OCCURRED = "ERROR_OCCURRED";
    public static final String FLAG_SESSION_FINISHED = "SESSION_FINISHED";
    public static final String FLAG_SESSION_CANCELLED = "SESSION_CANCELLED";

    public static final String REG_DEPENDENCIES = "dependencies";
    public static final String REGISTRY_PREFIX_EXPECTED_VALUES_OF = "expected-values-of--";

    public static final String COMMANDS_IN_PROGRESS = "commands-in-progress";
    public static final String PROCESSED_COMMANDS = "processed-commands";
    public static final String FAILED_COMMANDS = "failed-commands";
    public static final String COMMAND_IN_PROGRESS_PREFIX = "command:";
    public static final String LOCKED_EVENTS = "locked-events";
    public static final String UPDATE_HISTORY = "update-history";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
    }

    private final InstanceId instanceId;
    private final SessionId id;
    private final LocalDateTime creationTimestamp;
    private final Map<String, String> instanceConfiguration;
    private SessionProcessingState processingState;
    private Map<String, String> state;

    @JsonIgnore
    private SessionUpdateHistory sessionUpdateHistory;

    public Session(InstanceId instanceId, SessionId id) {
        this(instanceId, id, LocalDateTime.now(ZoneId.systemDefault()), Collections.emptyMap());
    }

    @ConstructorProperties({"instanceId", "id", "creationTimestamp", "instanceConfiguration"})
    public Session(InstanceId instanceId, SessionId id, LocalDateTime creationTimestamp, Map<String, String> instanceConfiguration) {
        Objects.requireNonNull(instanceConfiguration, "instanceConfiguration must be not null!");

        this.instanceId = Objects.requireNonNull(instanceId, "instanceId must be not null!");
        this.id = Objects.requireNonNull(id, "id must be not null!");
        this.instanceConfiguration = Map.copyOf(instanceConfiguration);
        this.creationTimestamp = creationTimestamp;
    }

    public Optional<SessionUpdateHistory> getSessionUpdateHistory() {
        return sessionUpdateHistory == null ? Optional.empty() : Optional.of(sessionUpdateHistory);
    }

    public void setSessionUpdateHistory(SessionUpdateHistory sessionUpdateHistory) {
        this.sessionUpdateHistory = sessionUpdateHistory;
    }

    public static void updateProcessingState(Session session, Consumer<SessionProcessingState> consumer) {
        try {
            SessionProcessingState processingState = session.getProcessingState();
            if (processingState == null) {
                LOGGER.warn("Missing initialized SessionProcessingState in session {}!", session);
                return;
            }

            consumer.accept(processingState);

            updateGlobalFields(session, processingState);
            session.setProcessingState(processingState);

            Optional<SessionUpdateHistory> updateHistory = session.getSessionUpdateHistory();
            if (updateHistory.isPresent()) {
                updateHistory.get().add(
                    session.getId(),
                    new SessionUpdateProcessingStateOperation(processingState),
                    "updateProcessingState()"
                );
            }
            else {
                LOGGER.warn("No updateHistory found in session {}!", session);
            }
        }
        catch (Exception ex) {
            LOGGER.error("Error updating session processing state in {}!", session, ex);
        }
    }

    public static void updateProcessingState(
        Session session,
        InternalEvent event,
        BiConsumer<SessionProcessingState, InternalEvent> consumer
    ) {
        try {
            SessionProcessingState processingState = session.getProcessingState();
            if (processingState == null) {
                LOGGER.warn("Missing initialized SessionProcessingState in session {}!", session);
                return;
            }

            consumer.accept(processingState, event);

            updateGlobalFields(session, processingState);
            session.setProcessingState(processingState);

            Optional<SessionUpdateHistory> updateHistory = session.getSessionUpdateHistory();
            if (updateHistory.isPresent()) {
                updateHistory.get().add(
                    session.getId(),
                    new SessionUpdateProcessingStateOperation(processingState),
                    "updateProcessingState() for event " + event.getId()
                );
            }
            else {
                LOGGER.warn("No updateHistory found in session {}!", session);
            }
        }
        catch (Exception ex) {
            LOGGER.error("Error updating session processing state in {}!", session, ex);
        }
    }

    public static void startProgress(Session session, int progressMaxValue) {
        try {
            SessionProcessingState processingState = session.getProcessingState();
            if (processingState == null) {
                LOGGER.warn("Missing initialized SessionProcessingState in session {}!", session);
                return;
            }

            processingState.setProgressMaxValue(progressMaxValue);
            session.setProcessingState(processingState);

            Optional<SessionUpdateHistory> updateHistory = session.getSessionUpdateHistory();
            if (updateHistory.isPresent()) {
                updateHistory.get().add(
                    session.getId(),
                    new SessionSetProgressMaxValueOperation(progressMaxValue)
                );
            }
            else {
                LOGGER.warn("No updateHistory found in session {}!", session);
            }
        }
        catch (Exception ex) {
            LOGGER.error("Error starting session progress in {}!", session, ex);
        }
    }

    public static void updateProgress(Session session, boolean success) {
        try {
            SessionProcessingState processingState = session.getProcessingState();
            if (processingState == null) {
                LOGGER.warn("Missing initialized SessionProcessingState in session {}!", session);
                return;
            }

            processingState.setProgressCurrentStep(processingState.getProgressCurrentStep() + 1);

            Optional<SessionUpdateHistory> updateHistory = session.getSessionUpdateHistory();
            if (updateHistory.isPresent()) {
                updateHistory.get().add(
                    session.getId(),
                    new SessionUpdateProgressOperation()
                );
            }
            else {
                LOGGER.warn("No updateHistory found in session {}!", session);
            }

            if (!success) {
                processingState.setProgressFailedSteps(processingState.getProgressFailedSteps() + 1);

                session.getSessionUpdateHistory().ifPresent(it -> it.add(
                    session.getId(),
                    new SessionUpdateFailedProgressOperation()
                ));
            }

            updateGlobalFields(session, processingState);
            session.setProcessingState(processingState);
        }
        catch (Exception ex) {
            LOGGER.error("Error updating session progress in {}!", session, ex);
        }
    }

    private static void updateGlobalFields(Session session, SessionProcessingState state) {
        state.setLastUpdated(LocalDateTime.now(ZoneId.systemDefault()));
        state.getFlags().put(FLAG_ERROR_OCCURRED, session.hasFailedFlag());
        state.getFlags().put(FLAG_SESSION_CANCELLED, session.hasCancelledFlag());
        state.getFlags().put(FLAG_SESSION_FINISHED, session.hasFinishedFlag());
        state.setStateAttributes(session.getState());
    }

    public InstanceId getInstanceId() {
        return instanceId;
    }

    public SessionId getId() {
        return id;
    }

    public LocalDateTime getCreationTimestamp() {
        return creationTimestamp;
    }

    public Map<String, String> getInstanceConfiguration() {
        return instanceConfiguration;
    }

    public Map<String, String> getState() {
        return state;
    }

    public void setState(Map<String, String> state) {
        this.state = state;
    }

    public SessionProcessingState getProcessingState() {
        return processingState;
    }

    public void setProcessingState(SessionProcessingState processingState) {
        this.processingState = processingState;

        Optional<SessionUpdateHistory> updateHistory = getSessionUpdateHistory();
        if (updateHistory.isPresent()) {
            updateHistory.get().add(
                id,
                new SessionUpdateProcessingStateOperation(processingState),
                "setProcessingState()"
            );
        }
        else {
            LOGGER.warn("No updateHistory found in session {}!", id);
        }
    }

    public Optional<String> getStateValue(String key) {
        if (state == null) {
            LOGGER.debug("No session state available in session {}!", id);
            return Optional.empty();
        }

        Objects.requireNonNull(key, "key must not be null!");

        LOGGER.debug("Returning state value for key '{}' from session {}", key, id);
        return Optional.ofNullable(state.get(key));
    }

    public void setStateValue(String key, String value) {
        Objects.requireNonNull(key, "key must not be null!");
        if (state == null) {
            if (value == null) {
                return;
            }
            state = new HashMap<>();
        }

        if (value == null) {
            state.remove(key);
        } else {
            state.put(key, value);
        }

        if (sessionUpdateHistory != null) {
            sessionUpdateHistory.add(
                id,
                new SessionUpdateStateValueOperation(key, value)
            );
        }
    }

    public <T> T getStateMandatoryBean(String key, Class<T> beanClass) {
        T bean = getStateBean(key, beanClass);
        if (bean == null) {
            throw new IllegalArgumentException("No state bean found for key '" + key + "'!");
        }

        return bean;
    }

    public <T> T getStateBean(String key, Class<T> beanClass) {
        Optional<String> value = getStateValue(key);
        if (value.isEmpty()) {
            return null;
        }

        try {
            return OBJECT_MAPPER.readValue(value.get(), beanClass);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Error deserializing bean " + beanClass, ex);
        }
    }

    public <T> void setStateBean(String key, T bean) {
        Objects.requireNonNull(bean, "bean must not be null!");

        try {
            String value = OBJECT_MAPPER.writeValueAsString(bean);
            setStateValue(key, value);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Error serializing bean " + bean, ex);
        }
    }

    public Set<String> getStateRegistry(String key) {
        Optional<String> oRegistry = getStateValue(key);
        if (oRegistry.isEmpty()) {
            return new HashSet<>();
        }

        String value = oRegistry.get();
        if (checkTrimEmpty(value)) {
            return new HashSet<>();
        }

        return new HashSet<>(Arrays.asList(value.split(",")));
    }

    private boolean checkTrimEmpty(String str) {
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public void setStateRegistry(String key, Set<String> registry, Set<String> expectedRegistry) {
        setStateRegistry(key, registry);
        if (expectedRegistry != null && !expectedRegistry.isEmpty()) {
            setExpectedStateRegistry(key, expectedRegistry);
        }
    }

    public void setStateRegistry(String key, Set<String> registry) {
        if (registry == null) {
            setStateValue(key, null);
            return;
        }

        StringBuilder builder = new StringBuilder();
        for (String entry : registry) {
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(entry);
        }
        setStateValue(key, builder.toString());
    }

    public void setExpectedStateRegistry(String key, Set<String> expectedValues) {
        setStateRegistry(REGISTRY_PREFIX_EXPECTED_VALUES_OF + key, expectedValues);
    }

    public Set<String> extendStateRegistry(String key, String value) {
        Set<String> registry = getStateRegistry(key);
        registry.add(value);
        setStateRegistry(key, registry);

        String requiredValuesKey = REGISTRY_PREFIX_EXPECTED_VALUES_OF + key;
        if (state.containsKey(requiredValuesKey)) {
            Set<String> expectedValues = getStateRegistry(requiredValuesKey);
            expectedValues.remove(value);
            setStateRegistry(requiredValuesKey, expectedValues);
        }

        return registry;
    }

    public Set<String> getExpectedStateRegistry(String key) {
        String expectedKey = REGISTRY_PREFIX_EXPECTED_VALUES_OF + key;
        if (!state.containsKey(expectedKey)) {
            return Collections.emptySet();
        }

        return getStateRegistry(expectedKey);

    }

    public boolean isRegistryComplete(String key) {
        String expectedKey = REGISTRY_PREFIX_EXPECTED_VALUES_OF + key;
        if (!state.containsKey(expectedKey)) {
            return false;
        }

        return getStateRegistry(expectedKey).isEmpty();
    }

    public Set<String> reduceStateRegistry(String key, String value) {
        Set<String> registry = getStateRegistry(key);
        if (registry.remove(value)) {
            setStateRegistry(key, registry);
        }

        return registry;
    }

    // region dependencies

    @JsonIgnore
    public void setExpectedDependencies(String... dependencies) {
        Objects.requireNonNull(dependencies, "dependencies must not be null!");
        setStateRegistry(REG_DEPENDENCIES, Collections.emptySet(), new HashSet<>(Set.of(dependencies)));
    }

    @JsonIgnore
    public void setExpectedDependencies(Set<String> dependencies) {
        Objects.requireNonNull(dependencies, "dependencies must not be null!");
        setStateRegistry(REG_DEPENDENCIES, Collections.emptySet(), new HashSet<>(dependencies));
    }

    public void registerDependency(String dependency) {
        Objects.requireNonNull(dependency, "dependency must not be null!");
        extendStateRegistry(REG_DEPENDENCIES, dependency);
    }

    @JsonIgnore
    public Optional<Set<String>> getMissingDependencies() {
        Set<String> dependencies = getStateRegistry(REG_DEPENDENCIES);
        if (dependencies.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(dependencies);
    }

    // endregion

    public void setCommandExecutionId(String commandId) {
        setStateRegistry(COMMANDS_IN_PROGRESS, new HashSet<>(Collections.singleton(commandId)));
    }

    public void setCommandExecutionId(String commandId, String messageRegistry) {
        setStateRegistry(COMMANDS_IN_PROGRESS, new HashSet<>(Collections.singleton(commandId)));

        extendStateRegistry(messageRegistry, commandId);
    }

    public void addCommandExecutionId(String commandId) {
        extendStateRegistry(COMMANDS_IN_PROGRESS, commandId);
    }

    public void addCommandExecutionId(String commandId, String messageRegistry) {
        extendStateRegistry(COMMANDS_IN_PROGRESS, commandId);

        extendStateRegistry(messageRegistry, commandId);
    }

    public void addLabeledCommandExecutionId(String commandId, String label) {
        extendStateRegistry(COMMANDS_IN_PROGRESS, commandId);

        setStateValue(COMMAND_IN_PROGRESS_PREFIX + commandId, label);
    }

    public String findLabeledCommandExecutionId(InternalEvent event, String label) {
        LOGGER.debug("Searching for session attribute with command label '{}' in event {}", label, event);

        for (Map.Entry<String, String> entry : state.entrySet()) {
            String value = entry.getValue();
            if (label.equals(value)) {
                String key = entry.getKey();
                return key.substring(COMMAND_IN_PROGRESS_PREFIX.length());
            }
        }

        return null;
    }

    public void completeCommandExecution(String commandId) {
        if (!getStateRegistry(COMMANDS_IN_PROGRESS).contains(commandId)) {
            LOGGER.debug("Not a relevant command-id '{}'", commandId);
            return;
        }

        reduceStateRegistry(COMMANDS_IN_PROGRESS, commandId);
    }

    public void completeLabeledCommandExecution(InternalEvent event, String label) {
        String commandId = findLabeledCommandExecutionId(event, label);
        if (!getStateRegistry(COMMANDS_IN_PROGRESS).contains(commandId)) {
            LOGGER.debug("Not a relevant command-id '{}'", commandId);
            return;
        }

        reduceStateRegistry(COMMANDS_IN_PROGRESS, commandId);
    }

    public void lockEvent(String eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null!");
        extendStateRegistry(LOCKED_EVENTS, eventId);
    }

    public boolean isEventLocked(String eventId) {
        Objects.requireNonNull(eventId, "eventId must not be null!");
        return getStateRegistry(LOCKED_EVENTS).contains(eventId);
    }

    public boolean getStateFlag(String key) {
        Optional<String> oValue = getStateValue(key);
        return oValue.filter(Boolean::parseBoolean).isPresent();
    }

    public void setStateFlag(String key, boolean flag) {
        setStateValue(key, String.valueOf(flag));
    }

    public boolean hasFailedFlag() {
        return getStateFlag(FLAG_ERROR_OCCURRED);
    }

    public void setFailedFlag() {
        setStateFlag(FLAG_ERROR_OCCURRED, true);
    }

    public void resetFailedFlag() {
        setStateFlag(FLAG_ERROR_OCCURRED, false);
    }

    public boolean hasFinishedFlag() {
        return getStateFlag(FLAG_SESSION_FINISHED);
    }

    public void setFinishedFlag() {
        setStateFlag(FLAG_SESSION_FINISHED, true);
    }

    public void resetFinishedFlag() {
        setStateFlag(FLAG_SESSION_FINISHED, false);
    }

    public boolean hasCancelledFlag() {
        return Boolean.parseBoolean(getStateValue(FLAG_SESSION_CANCELLED).orElse("false"));
    }

    public void setCancelledFlag() {
        setStateFlag(FLAG_SESSION_CANCELLED, true);
    }

    @JsonIgnore
    public boolean isSuccessful() {
        return !(hasCancelledFlag() || hasFailedFlag());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Session)) return false;
        Session session = (Session) o;
        return Objects.equals(instanceId, session.instanceId)
            && Objects.equals(id, session.id)
            && Objects.equals(instanceConfiguration, session.instanceConfiguration)
            && Objects.equals(state, session.state)
            && Objects.equals(processingState, session.processingState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, id, instanceConfiguration, state, processingState);
    }

    @Override
    public String toString() {
        return "Session{" +
            "instanceId=" + instanceId +
            ", id=" + id +
            ", creationTimestamp=" + creationTimestamp +
            ", instanceConfiguration=" + instanceConfiguration +
            ", state=" + state +
            '}';
    }

    @Override
    @SuppressWarnings({"PMD.ProperCloneImplementation", "MethodDoesntCallSuperMethod"})
    public Session clone() {
        // instanceConfiguration is unmodifiable
        Session clone = new Session(
            instanceId, id, creationTimestamp, instanceConfiguration
        );

        if (state != null) {
            clone.state = new HashMap<>(state);
        }
        if (processingState != null) {
            clone.processingState = processingState.clone();
        }
        clone.sessionUpdateHistory = sessionUpdateHistory;
        return clone;
    }
}
