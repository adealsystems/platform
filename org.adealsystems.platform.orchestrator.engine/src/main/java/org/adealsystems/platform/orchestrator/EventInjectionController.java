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

package org.adealsystems.platform.orchestrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.adealsystems.platform.orchestrator.InternalEventHandlerRunnable.INTERNAL_SESSION_STATE_IDS;

@RestController
@RequestMapping("/")
public class EventInjectionController {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventInjectionController.class);

    private final InternalEventSender eventSender;

    private final TimestampFactory timestampFactory;

    public EventInjectionController(
        @Qualifier("raw-event-sender") InternalEventSender eventSender,
        TimestampFactory timestampFactory
    ) {
        LOGGER.info("Initializing controller {}", EventInjectionController.class);
        this.eventSender = Objects.requireNonNull(eventSender, "eventSender must not be null!");
        this.timestampFactory = Objects.requireNonNull(timestampFactory, "timestampFactory must not be null!");
    }

    @PostMapping("/send-event")
    public InternalEvent sendEvent(@RequestBody InternalEvent event) {
        LOGGER.info("Processing send-event request {}", event);

        return internalSendEvent(event);
    }

    @PostMapping("/send-events")
    public List<InternalEvent> sendEvents(@RequestBody List<InternalEvent> events) {
        LOGGER.info("Processing send-events request {}", events);

        Objects.requireNonNull(events, "events must not be null!");

        for (InternalEvent event : events) {
            if (event == null) {
                continue;
            }

            if (isInvalidEvent(event)) {
                throw new SessionEventNotAllowedException(event);
            }
        }

        List<InternalEvent> result = new ArrayList<>();
        for (InternalEvent event : events) {
            if (event == null) {
                continue;
            }

            result.add(internalSendEvent(event));
        }

        return result;
    }

    private static boolean isInvalidEvent(InternalEvent event) {
        return event.getType() == InternalEventType.SESSION && INTERNAL_SESSION_STATE_IDS.contains(event.getId());
    }

    private InternalEvent internalSendEvent(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");

        if (isInvalidEvent(event)) {
            throw new SessionEventNotAllowedException(event);
        }

        InstanceId instanceId = event.getInstanceId();
        if (instanceId != null) {
            LOGGER.info("Removing instanceId from event {}", event);
            event.setInstanceId(null);
        }

        SessionId sessionId = event.getSessionId();
        if (sessionId != null) {
            LOGGER.info("Removing sessionId from event {}", event);
            event.setSessionId(null);
        }

        InternalEvent clonedEvent = event.clone();

        if (clonedEvent.getTimestamp() == null) {
            LOGGER.info("Extending event with current timestamp: {}", clonedEvent);
            clonedEvent.setTimestamp(timestampFactory.createTimestamp());
        }

        LOGGER.info("Sending event {}", event);
        eventSender.sendEvent(clonedEvent);

        return event;
    }

    @ResponseStatus(HttpStatus.FORBIDDEN)
    static class SessionEventNotAllowedException extends RuntimeException {

        private static final long serialVersionUID = 2564715128740160336L;

        private final InternalEvent event;

        SessionEventNotAllowedException(InternalEvent event) {
            this.event = event;
        }

        public InternalEvent getEvent() {
            return event;
        }

        @Override
        public String toString() {
            return "SessionEventNotAllowedException{" +
                "event=" + event +
                "}";
        }
    }
}
