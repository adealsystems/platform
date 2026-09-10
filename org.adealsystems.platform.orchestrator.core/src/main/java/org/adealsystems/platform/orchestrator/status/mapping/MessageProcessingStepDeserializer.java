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

import org.adealsystems.platform.orchestrator.InternalEvent;
import org.adealsystems.platform.orchestrator.status.MessageProcessingStep;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

public class MessageProcessingStepDeserializer extends StdDeserializer<MessageProcessingStep> {
    protected MessageProcessingStepDeserializer() {
        super(MessageProcessingStep.class);
    }

    @Override
    public MessageProcessingStep deserialize(JsonParser parser, DeserializationContext ctx) {
        RootNode rootNode = parser.readValueAs(RootNode.class);
        return new MessageProcessingStep(rootNode.success, rootNode.event, rootNode.instanceRef, rootNode.message);
    }

    private static final class RootNode {
        public boolean success;
        public InternalEvent event;
        public String message;
        public String instanceRef;
    }
}
