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

package org.adealsystems.platform.orchestrator.status.mapping;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.adealsystems.platform.orchestrator.InternalEvent;
import org.adealsystems.platform.orchestrator.status.TimerProcessingStep;

import java.io.IOException;

public class TimerProcessingStepDeserializer extends StdDeserializer<TimerProcessingStep> {
    private static final long serialVersionUID = 2333073101219808266L;

    protected TimerProcessingStepDeserializer() {
        this(null);
    }

    protected TimerProcessingStepDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public TimerProcessingStep deserialize(JsonParser parser, DeserializationContext ctx) throws IOException, JacksonException {
        Root root = parser.readValueAs(Root.class);
        return new TimerProcessingStep(root.success, root.event, root.message, root.name);
    }

    private static class Root { // NOPMD
        public boolean success;
        public InternalEvent event;
        public String message;
        public String name;
    }
}
