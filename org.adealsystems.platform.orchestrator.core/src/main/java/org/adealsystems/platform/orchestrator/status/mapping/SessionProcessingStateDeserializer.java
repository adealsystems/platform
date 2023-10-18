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

package org.adealsystems.platform.orchestrator.status.mapping;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.adealsystems.platform.orchestrator.RunSpecification;
import org.adealsystems.platform.orchestrator.status.ProcessingStep;
import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.adealsystems.platform.orchestrator.status.State;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class SessionProcessingStateDeserializer extends StdDeserializer<SessionProcessingState> {
    private static final long serialVersionUID = -4641250437409122345L;

    protected SessionProcessingStateDeserializer() {
        this(null);
    }

    protected SessionProcessingStateDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public SessionProcessingState deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
        Root root = parser.readValueAs(Root.class);
        return new SessionProcessingState(
            root.runSpec,
            root.state,
            root.message,
            root.started,
            root.terminated,
            root.lastUpdated,
            root.progressMaxValue,
            root.progressCurrentStep,
            root.progressFailedSteps,
            root.flags,
            root.steps
        );
    }

    private static class Root { // NOPMD
        public RunSpecification runSpec;
        public State state;
        public String message;
        public List<ProcessingStep> steps;
        public Map<String, Boolean> flags;
        public LocalDateTime started;
        public LocalDateTime terminated;
        public LocalDateTime lastUpdated;
        public int progressMaxValue = 1;
        public int progressCurrentStep = 0;
        public int progressFailedSteps = 0;
    }
}
