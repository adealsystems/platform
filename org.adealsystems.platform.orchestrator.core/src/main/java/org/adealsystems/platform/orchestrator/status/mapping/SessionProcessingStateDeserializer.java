/*
 * Copyright 2020-2026 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator.status.mapping;

import org.adealsystems.platform.orchestrator.RunSpecification;
import org.adealsystems.platform.orchestrator.status.ProcessingStep;
import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.adealsystems.platform.orchestrator.status.State;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class SessionProcessingStateDeserializer extends StdDeserializer<SessionProcessingState> {
    protected SessionProcessingStateDeserializer() {
        super(SessionProcessingState.class);
    }


    @Override
    public SessionProcessingState deserialize(JsonParser parser, DeserializationContext ctx) {
        RootNode rootNode = parser.readValueAs(RootNode.class);
        return new SessionProcessingState(
            rootNode.runSpec,
            rootNode.configuration,
            rootNode.state,
            rootNode.message,
            rootNode.started,
            rootNode.terminated,
            rootNode.lastUpdated,
            rootNode.progressMaxValue,
            rootNode.progressCurrentStep,
            rootNode.progressFailedSteps,
            rootNode.flags,
            rootNode.steps,
            rootNode.stateAttributes
        );
    }

    @SuppressWarnings("PMD.ImmutableField")
    private static final class RootNode {
        public RunSpecification runSpec;
        public Map<String, String> configuration;
        public State state;
        public String message;
        public List<ProcessingStep> steps;
        public Map<String, Boolean> flags;
        public Map<String, String> stateAttributes;
        public LocalDateTime started;
        public LocalDateTime terminated;
        public LocalDateTime lastUpdated;
        public int progressMaxValue = 1;
        public int progressCurrentStep = 0;
        public int progressFailedSteps = 0;
    }
}
