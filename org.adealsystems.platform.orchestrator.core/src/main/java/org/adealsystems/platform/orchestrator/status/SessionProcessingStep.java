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
import org.adealsystems.platform.orchestrator.SessionEventConstants;

import java.util.Objects;

public class SessionProcessingStep extends EventProcessingStep {
    private static final long serialVersionUID = -2205103557789816206L;

    private final String instanceRef;

    public static SessionProcessingStep success(InternalEvent event, String instanceRef) {
        return new SessionProcessingStep(true, event, instanceRef, buildDefaultMessage(event));
    }

    public static SessionProcessingStep failed(InternalEvent event, String instanceRef) {
        return new SessionProcessingStep(false, event, instanceRef, buildDefaultMessage(event));
    }

    public static SessionProcessingStep failed(InternalEvent event, String instanceRef, String message) {
        return new SessionProcessingStep(false, event, instanceRef, message);
    }

    private static String buildDefaultMessage(InternalEvent event) {
        String instanceId = event.getAttributeValue(SessionEventConstants.INSTANCE_ID_ATTRIBUTE_NAME).orElse("");
        String lifecycle = event.getAttributeValue(SessionEventConstants.SESSION_LIFECYCLE_ATTRIBUTE_NAME).orElse("");
        switch (lifecycle) {
            case SessionEventConstants.SESSION_STOP:
                return "Session " + instanceId + " was closed";
            case SessionEventConstants.SESSION_START:
                return  "Session " + instanceId + " started";
            case SessionEventConstants.SESSION_RESUME:
                return  "Session " + instanceId + " was resumed";
            case SessionEventConstants.SESSION_STATE:
                return  "Session " + instanceId + " has been updated";
            default:
                return null;
        }
    }

    public SessionProcessingStep(boolean success, InternalEvent event, String instanceRef, String message) {
        super(success, event, message);
        this.instanceRef = instanceRef;
    }

    public String getInstanceRef() {
        return instanceRef;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SessionProcessingStep)) return false;
        if (!super.equals(o)) return false;
        SessionProcessingStep that = (SessionProcessingStep) o;
        return Objects.equals(instanceRef, that.instanceRef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), instanceRef);
    }

    @Override
    public String toString() {
        return "SessionProcessingStep{" +
                "instanceRef='" + instanceRef + '\'' +
                "} " + super.toString();
    }
}
