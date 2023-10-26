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
import org.adealsystems.platform.orchestrator.InternalEvent;
import org.adealsystems.platform.orchestrator.status.MessageProcessingStep;

import java.io.IOException;

public class MessageProcessingStepDeserializer extends StdDeserializer<MessageProcessingStep> {
    private static final long serialVersionUID = 2333073101219808266L;

    protected MessageProcessingStepDeserializer() {
        this(null);
    }

    protected MessageProcessingStepDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public MessageProcessingStep deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
        Root root = parser.readValueAs(Root.class);
        return new MessageProcessingStep(root.success, root.event, root.instanceRef, root.message);
    }

    private static class Root { // NOPMD
        public boolean success;
        public InternalEvent event;
        public String message;
        public String instanceRef;
    }
}
