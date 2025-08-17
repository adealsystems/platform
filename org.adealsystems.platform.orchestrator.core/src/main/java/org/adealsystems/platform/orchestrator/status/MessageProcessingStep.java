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


import org.adealsystems.platform.orchestrator.InternalEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public class MessageProcessingStep extends EventProcessingStep {
    private static final long serialVersionUID = -5021237990561079026L;

    private final String instanceRef;

    public static MessageProcessingStep success(InternalEvent event, String instanceRef) {
        return new MessageProcessingStep(true, event, instanceRef, buildDefaultMessage(event));
    }

    public static MessageProcessingStep failed(InternalEvent event, String instanceRef) {
        return new MessageProcessingStep(false, event, instanceRef, buildDefaultMessage(event));
    }

    public static MessageProcessingStep failed(InternalEvent event, String instanceRef, String message) {
        return new MessageProcessingStep(false, event, instanceRef, message);
    }

    private static String buildDefaultMessage(InternalEvent event) {
        StringBuilder builder = new StringBuilder();
        builder.append(event.getId());
        ConcurrentMap<String, String> attributes = event.getAttributes();
        if (attributes != null) {
            boolean first = true;
            builder.append(" (");
            List<String> keys = new ArrayList<>(attributes.keySet());
            Collections.sort(keys);
            for (String key : keys) {
                String value = attributes.get(key);
                if (value != null && !value.isEmpty()) {
                    if (!first) {
                        builder.append(", ");
                    }
                    builder.append(key).append("='").append(value).append('\'');
                    first = false;
                }
            }
            builder.append(')');
        }
        return builder.toString();
    }

    public MessageProcessingStep(boolean success, InternalEvent event, String instanceRef, String message) {
        super(success, event, message);
        this.instanceRef = instanceRef;
    }

    public String getInstanceRef() {
        return instanceRef;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessageProcessingStep)) return false;
        if (!super.equals(o)) return false;
        MessageProcessingStep that = (MessageProcessingStep) o;
        return Objects.equals(instanceRef, that.instanceRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), instanceRef);
    }

    @Override
    public String toString() {
        return "MessageProcessingStep{" +
                "instanceRef='" + instanceRef + '\'' +
                "} " + super.toString();
    }
}
