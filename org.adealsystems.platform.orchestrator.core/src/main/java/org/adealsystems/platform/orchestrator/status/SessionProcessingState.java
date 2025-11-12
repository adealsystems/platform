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

package org.adealsystems.platform.orchestrator.status;

import org.adealsystems.platform.orchestrator.RunSpecification;
import org.adealsystems.platform.orchestrator.Session;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class SessionProcessingState implements Cloneable, Serializable {
    private static final long serialVersionUID = -4771183871755310172L;

    private final RunSpecification runSpec;
    private State state;
    private String message;
    private Map<String, String> configuration;
    private final List<ProcessingStep> steps;
    private LocalDateTime started;
    private LocalDateTime terminated;
    private LocalDateTime lastUpdated;
    private int progressMaxValue;
    private int progressCurrentStep;
    private int progressFailedSteps;
    private Map<String, Boolean> flags;
    private Map<String, String> stateAttributes;

    public static SessionProcessingState clone(SessionProcessingState state) {
        return new SessionProcessingState(
            state.getRunSpec(),
            state.getConfiguration(),
            state.getState(),
            state.getMessage(),
            state.getStarted(),
            state.getTerminated(),
            state.getLastUpdated(),
            state.getProgressMaxValue(),
            state.getProgressCurrentStep(),
            state.getProgressFailedSteps(),
            state.getFlags(),
            null,
            state.getStateAttributes()
        );
    }

    public static String buildTerminationMessage(Session session) {
        StringBuilder msg = new StringBuilder(45);

        if (session.hasFailedFlag()) {
            msg.append("Terminated by failed flag");
        }
        else if(session.hasCancelledFlag()) {
            msg.append("Terminated by cancelled flag");
        }
        else if(session.hasFinishedFlag()) {
            msg.append("Terminated by finished flag");
        }
        else {
            msg.append("Terminated regular");
        }

        msg.append(" after");

        SessionProcessingState state = session.getProcessingState();
        Duration duration = Duration.between(
            state.started,
            state.terminated == null ? LocalDateTime.now(ZoneId.systemDefault()) : state.terminated
        );

        long hours = duration.toHours();
        if (hours > 0) {
            msg.append(' ').append(hours).append(' ').append(hours == 1 ? "hour" : "hours");
            duration = duration.minusHours(hours);
        }

        long minutes = duration.toMinutes();
        if (minutes > 0) {
            msg.append(' ').append(minutes).append(' ').append(minutes == 1 ? "minute" : "minutes");
            duration = duration.minusMinutes(minutes);
        }

        long seconds = duration.getSeconds();
        if (seconds > 0) {
            msg.append(' ').append(seconds).append(' ').append(seconds == 1 ? "second" : "seconds");
        }
        else {
            int nanos = duration.getNano();
            msg.append(' ').append(nanos).append(' ').append(nanos == 1 ? "nano" : "nanos");
        }

        return msg.toString();
    }

    public SessionProcessingState(RunSpecification runSpec) {
        this.state = State.READY_TO_RUN;
        this.runSpec = runSpec;
        this.steps = new ArrayList<>();
        this.configuration = null;
        this.progressMaxValue = 1;
        this.progressCurrentStep = 0;
        this.progressFailedSteps = 0;
        this.flags = new HashMap<>();
        this.stateAttributes = new HashMap<>();
    }

    @SuppressWarnings("PMD.ExcessiveParameterList")
    public SessionProcessingState(
        RunSpecification runSpec,
        Map<String, String> config,
        State state,
        String message,
        LocalDateTime started,
        LocalDateTime terminated,
        LocalDateTime lastUpdates,
        int progressMaxValue,
        int progressCurrentStep,
        int progressFailedSteps,
        Map<String, Boolean> flags,
        List<ProcessingStep> steps,
        Map<String, String> attributes
    ) {
        this.runSpec = runSpec;
        this.configuration = config == null ? null : new HashMap<>(config);
        this.state = state;
        this.message = message;
        this.steps = steps;
        this.started = started;
        this.terminated = terminated;
        this.lastUpdated = lastUpdates;
        this.progressMaxValue = progressMaxValue;
        this.progressCurrentStep = progressCurrentStep;
        this.progressFailedSteps = progressFailedSteps;
        this.flags = flags == null ? new HashMap<>() : flags;
        this.stateAttributes = attributes == null ? new HashMap<>() : new HashMap<>(attributes);
    }

    public void addStep(EventProcessingStep step) {
        Objects.requireNonNull(step, "step must not be null!");

        steps.add(step);
    }

    public RunSpecification getRunSpec() {
        return runSpec;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<ProcessingStep> getSteps() {
        return steps;
    }

    public LocalDateTime getStarted() {
        return started;
    }

    public void setStarted(LocalDateTime started) {
        this.started = started;
    }

    public LocalDateTime getTerminated() {
        return terminated;
    }

    public void setTerminated(LocalDateTime terminated) {
        this.terminated = terminated;
    }

    public int getProgressMaxValue() {
        return progressMaxValue;
    }

    public void setProgressMaxValue(int progressMaxValue) {
        this.progressMaxValue = progressMaxValue;
    }

    public int getProgressCurrentStep() {
        return progressCurrentStep;
    }

    public void setProgressCurrentStep(int progressCurrentStep) {
        this.progressCurrentStep = progressCurrentStep;
    }

    public int getProgressFailedSteps() {
        return progressFailedSteps;
    }

    public void setProgressFailedSteps(int progressFailedSteps) {
        this.progressFailedSteps = progressFailedSteps;
    }

    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(LocalDateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public Map<String, Boolean> getFlags() {
        return flags;
    }

    public void setFlags(Map<String, Boolean> flags) {
        this.flags = flags;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    public Map<String, String> getStateAttributes() {
        return stateAttributes;
    }

    public void setStateAttributes(Map<String, String> stateAttributes) {
        this.stateAttributes = stateAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SessionProcessingState)) return false;
        SessionProcessingState that = (SessionProcessingState) o;
        return Objects.equals(runSpec, that.runSpec)
            && state == that.state
            && Objects.equals(configuration, that.configuration)
            && Objects.equals(message, that.message)
            && Objects.equals(steps, that.steps)
            && Objects.equals(started, that.started)
            && Objects.equals(terminated, that.terminated)
            && Objects.equals(lastUpdated, that.lastUpdated)
            && Objects.equals(progressMaxValue, that.progressMaxValue)
            && Objects.equals(progressCurrentStep, that.progressCurrentStep)
            && Objects.equals(progressFailedSteps, that.progressFailedSteps)
            && Objects.equals(stateAttributes, that.stateAttributes)
            && Objects.equals(flags, that.flags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            runSpec,
            configuration,
            state,
            message,
            steps,
            started,
            terminated,
            lastUpdated,
            progressMaxValue,
            progressCurrentStep,
            progressFailedSteps,
            flags,
            stateAttributes
        );
    }

    @Override
    public String toString() {
        return "SessionProcessingState{" +
            "runSpec=" + runSpec +
            ", configuration=" + configuration +
            ", state=" + state +
            ", message=" + message +
            ", steps=" + steps +
            ", started=" + started +
            ", terminated=" + terminated +
            ", lastUpdated=" + lastUpdated +
            ", progressMaxValue=" + progressMaxValue +
            ", progressCurrentStep=" + progressCurrentStep +
            ", progressFailedSteps=" + progressFailedSteps +
            ", flags=" + flags +
            ", stateAttributes=" + stateAttributes +
            '}';
    }

    @Override
    @SuppressWarnings({"PMD.ProperCloneImplementation", "MethodDoesntCallSuperMethod"})
    public SessionProcessingState clone() {
        List<ProcessingStep> cloneSteps = new ArrayList<>();
        if (steps != null) {
            cloneSteps.addAll(steps);
        }

        Map<String, Boolean> cloneFlags = new HashMap<>();
        if (flags != null) {
            cloneFlags.putAll(flags);
        }

        Map<String, String> cloneAttributes = new HashMap<>();
        if (stateAttributes != null) {
            cloneAttributes.putAll(stateAttributes);
        }

        return new SessionProcessingState(
            runSpec,
            configuration,
            state,
            message,
            started,
            terminated,
            lastUpdated,
            progressMaxValue,
            progressCurrentStep,
            progressFailedSteps,
            cloneFlags,
            cloneSteps,
            cloneAttributes
        );
    }
}
