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

import java.util.Objects;

public class EventProcessingStep implements ProcessingStep {
    private static final long serialVersionUID = 8583659551095886478L;

    private final boolean success; // NOPMD
    private final InternalEvent event;
    private final String message;

    public static EventProcessingStep success(InternalEvent event) {
        return new EventProcessingStep(true, event, event.getId() + " has arrived");
    }

    public static EventProcessingStep success(InternalEvent event, String message) {
        return new EventProcessingStep(true, event, message);
    }

    public static EventProcessingStep failed(InternalEvent event) {
        return new EventProcessingStep(false, event, "Error occurred while processing event " + event.getId());
    }

    public static EventProcessingStep failed(InternalEvent event, String message) {
        return new EventProcessingStep(false, event, message);
    }

    public EventProcessingStep(boolean success, InternalEvent event, String message) {
        this.event = event;
        this.success = success;
        this.message = message;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    @Override
    public InternalEvent getEvent() {
        return event;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventProcessingStep)) return false;
        EventProcessingStep that = (EventProcessingStep) o;
        return success == that.success
            && Objects.equals(event, that.event)
            && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(success, event, message);
    }

    @Override
    public String toString() {
        return "EventProcessingStep{" +
            "success=" + success +
            ", event=" + event +
            ", message='" + message + '\'' +
            '}';
    }
}
