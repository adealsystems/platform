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

package org.adealsystems.platform.orchestrator.registry;


import org.adealsystems.platform.orchestrator.InternalEvent;
import org.adealsystems.platform.orchestrator.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.adealsystems.platform.orchestrator.SessionEventConstants.INSTANCE_ID_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_LIFECYCLE;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_LIFECYCLE_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_RESUME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_START;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_STATE;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_STOP;
import static org.adealsystems.platform.orchestrator.registry.CommandExecutionCompletedMessageEvent.COMMAND_EXECUTION_COMPLETED_MESSAGE_PATTERN;
import static org.adealsystems.platform.orchestrator.registry.CommandExecutionCompletedMessageEvent.COMMAND_ID_GROUP_NAME;
import static org.adealsystems.platform.orchestrator.registry.CommandExecutionCompletedMessageEvent.LEGACY_COMMAND_EXECUTION_COMPLETED_MESSAGE_PREFIX;


public class EventRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventRegistry.class);

    private final Map<String, EventDescriptor> entries = new HashMap<>();

    private final Map<String, Set<FileEvent>> fileEventsByZone = new HashMap<>();
    private final Map<String, Set<ZoneDescriptor.FileDescriptor>> fileDescriptorsByZone = new HashMap<>();
    private final Set<CancelEvent> cancelEvents = new HashSet<>();
    private final Set<TimerEvent> timerEvents = new HashSet<>();
    private final Set<SessionEvent> sessionEvents = new HashSet<>();
    private final Set<MessageEvent> messageEvents = new HashSet<>();
    private final Set<CommandExecutionCompletedMessageEvent> commandExecutionCompletedMessageEvents = new HashSet<>();

    public void clear() {
        entries.clear();
    }

    public <T extends EventDescriptor> void addEntry(T entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        if (!entry.isValid()) {
            throw new IllegalArgumentException("Entry can't be added, because it isn't correct configured: " + entry);
        }

        String id = Objects.requireNonNull(entry.getId(), "event-id must not be null!");
        entries.put(id, entry);
    }

    public void init() {
        LOGGER.debug("Initializing from {}", entries);
        for (EventDescriptor entry : entries.values()) {
            if (CancelEvent.class.isAssignableFrom(entry.getClass())) {
                cancelEvents.add((CancelEvent) entry);
                continue;
            }

            if (TimerEvent.class.isAssignableFrom(entry.getClass())) {
                timerEvents.add((TimerEvent) entry);
                continue;
            }

            if (SessionEvent.class.isAssignableFrom(entry.getClass())) {
                sessionEvents.add((SessionEvent) entry);
                continue;
            }

            if (MessageEvent.class.isAssignableFrom(entry.getClass())) {
                messageEvents.add((MessageEvent) entry);
                continue;
            }

            if (CommandExecutionCompletedMessageEvent.class.isAssignableFrom(entry.getClass())) {
                commandExecutionCompletedMessageEvents.add((CommandExecutionCompletedMessageEvent) entry);
                continue;
            }

            if (FileEvent.class.isAssignableFrom(entry.getClass())) {
                FileEvent fileEvent = (FileEvent) entry;
                String zone = fileEvent.getZone();

                Set<FileEvent> zoneEvents = fileEventsByZone.get(zone);
                if (zoneEvents == null) {
                    zoneEvents = new HashSet<>(); // NOPMD
                    fileEventsByZone.put(zone, zoneEvents);
                }
                zoneEvents.add(fileEvent);

                Set<ZoneDescriptor.FileDescriptor> zoneDescriptors = fileDescriptorsByZone.get(zone);
                if (zoneDescriptors == null) {
                    zoneDescriptors = new HashSet<>(); // NOPMD
                    fileDescriptorsByZone.put(zone, zoneDescriptors);
                }
                zoneDescriptors.add(new ZoneDescriptor.FileDescriptor(
                    fileEvent.getPattern(),
                    fileEvent.getMetaName(),
                    fileEvent.getDataId()
                ));

                continue;
            }

            throw new IllegalStateException("Unknown/unexpected event descriptor " + entry);
        }

        if (LOGGER.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder() // NOPMD
                .append("\n\tcancel: ").append(cancelEvents.size())
                .append("\n\ttimer: ").append(timerEvents.size())
                .append("\n\tsession: ").append(sessionEvents.size())
                .append("\n\tmessage: ").append(messageEvents.size())
                .append("\n\tcommand-execution-completed-message: ").append(commandExecutionCompletedMessageEvents.size())
                .append("\n\tfile:");

            for (Map.Entry<String, Set<ZoneDescriptor.FileDescriptor>> entry : fileDescriptorsByZone.entrySet()) {
                builder.append("\n\t\t").append(entry.getKey()).append(": ").append(entry.getValue().size());
            }

            LOGGER.debug("Initialized descriptors:{}", builder);
        }
    }

    public Map<String, EventDescriptor> getEntries() {
        return entries;
    }

    public Set<ZoneDescriptor.FileDescriptor> getFileDescriptors(String zone) {
        return fileDescriptorsByZone.get(zone);
    }

    public Set<String> getCancelEventNames() {
        if (cancelEvents.isEmpty()) {
            return Collections.emptySet();
        }

        return cancelEvents.stream()
            .map(CancelEvent::getName)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    public Set<String> getTimerEventNames() {
        if (timerEvents.isEmpty()) {
            return Collections.emptySet();
        }

        return timerEvents.stream()
            .map(TimerEvent::getName)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    public Set<Pattern> getTimerEventPatterns() {
        if (timerEvents.isEmpty()) {
            return Collections.emptySet();
        }

        return timerEvents.stream()
            .map(TimerEvent::getPattern)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    public Set<String> getSessionEventRefs() {
        if (sessionEvents.isEmpty()) {
            return Collections.emptySet();
        }

        return sessionEvents.stream()
            .map(SessionEvent::getInstanceRef)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    public Optional<FileEvent> findFileEvent(Session session, InternalEvent event) {
        String eventId = event.getId();

        for (Set<FileEvent> zoneEvents : fileEventsByZone.values()) {
            if (zoneEvents == null || zoneEvents.isEmpty()) {
                continue;
            }

            for (FileEvent fileEvent : zoneEvents) {
                if (fileEvent.getPattern().matcher(eventId).matches()) {
                    BiFunction<Session, InternalEvent, Boolean> postValidator = fileEvent.getPostValidator();
                    if (postValidator != null && !postValidator.apply(session, event)) {
                        return Optional.empty();
                    }
                    return Optional.of(fileEvent);
                }
            }
        }

        return Optional.empty();
    }

    public Optional<FileEvent> findFileEvent(InternalEvent event, String zone) {
        String eventId = event.getId();

        Set<FileEvent> zoneEvents = fileEventsByZone.get(zone);
        for (FileEvent fileEvent : zoneEvents) {
            if (fileEvent.getPattern().matcher(eventId).matches()) {
                return Optional.of(fileEvent);
            }
        }

        return Optional.empty();
    }

    public Optional<SessionEvent> findSessionEvent(InternalEvent event) {
        if (!SESSION_LIFECYCLE.equals(event.getId())) {
            // Not a lifecycle session event
            return Optional.empty();
        }

        Optional<String> oInstanceId = event.getAttributeValue(INSTANCE_ID_ATTRIBUTE_NAME);
        if (!oInstanceId.isPresent()) {
            // Event does not have an instance-id attribute
            return Optional.empty();
        }
        String eventInstanceId = oInstanceId.get();

        for (SessionEvent sessionEvent : sessionEvents) {
            if (eventInstanceId.equals(sessionEvent.getInstanceRef())) {
                Optional<String> oSessionLifecycle = event.getAttributeValue(SESSION_LIFECYCLE_ATTRIBUTE_NAME);

                switch (sessionEvent.getType()) {
                    case ANY:
                        return Optional.of(sessionEvent);
                    case START:
                        return oSessionLifecycle.filter(SESSION_START::equals).map(s -> sessionEvent);
                    case STOP:
                        return oSessionLifecycle.filter(SESSION_STOP::equals).map(s -> sessionEvent);
                    case RESUME:
                        return oSessionLifecycle.filter(SESSION_RESUME::equals).map(s -> sessionEvent);
                    case CHANGE:
                        return oSessionLifecycle.filter(SESSION_STATE::equals).map(s -> sessionEvent);
                }
            }
        }

        return Optional.empty();
    }

    public Optional<MessageEvent> findMessageEvent(InternalEvent event) {
        String eventId = event.getId();

        // first try: search be id
        for (MessageEvent messageEvent : messageEvents) {
            if (eventId.equals(messageEvent.getName())) {
                return Optional.of(messageEvent);
            }
        }

        // second try: search by pattern match
        for (MessageEvent messageEvent : messageEvents) {
            Pattern pattern = messageEvent.getPattern();
            if (pattern != null && pattern.matcher(eventId).matches()) {
                return Optional.of(messageEvent);
            }
        }

        // not found
        return Optional.empty();
    }

    public Optional<CommandExecutionCompletedMessageEvent> findCommandExecutionCompletedMessageEvent(InternalEvent event, Session session) {
        LOGGER.debug("Searching for command-execution-completed message descriptor of '{}'", event);
        String eventId = event.getId();

        Matcher matcher = COMMAND_EXECUTION_COMPLETED_MESSAGE_PATTERN.matcher(eventId);
        if (!matcher.matches()) {
            LOGGER.debug("Event-ID '{}' does not match expected pattern {}", eventId, COMMAND_EXECUTION_COMPLETED_MESSAGE_PATTERN.pattern());
            // return Optional.empty();

            // TODO: remove legacy message support after migration!
            matcher = Pattern
                .compile(LEGACY_COMMAND_EXECUTION_COMPLETED_MESSAGE_PREFIX + "(?<" + COMMAND_ID_GROUP_NAME + ">.*)")
                .matcher(eventId);

            if (!matcher.matches()) {
                return Optional.empty();
            }
            // TODO: end
        }

        String commandId = matcher.group(COMMAND_ID_GROUP_NAME);
        if (commandId == null || commandId.isEmpty()) {
            LOGGER.debug("Command-ID isn't a part of event-ID '{}'", eventId);
            return Optional.empty();
        }

        LOGGER.debug("Searching for session registry with command-id '{}'", commandId);
        for (CommandExecutionCompletedMessageEvent messageEvent : commandExecutionCompletedMessageEvents) {
            String sessionRegistryName = messageEvent.getSessionRegistryName();
            LOGGER.debug("Verifying session registry '{}'", sessionRegistryName);
            Set<String> registry = session.getStateRegistry(sessionRegistryName);
            if (registry.contains(commandId)) {
                LOGGER.info("Found a session registry '{}' containing command-id '{}'", sessionRegistryName, commandId);
                return Optional.of(CommandExecutionCompletedMessageEvent.fromEvent(eventId, messageEvent));
            }

            // try the next configured command-execution-finished message
        }

        LOGGER.debug("No session registry containing command-id '{}' found", commandId);
        return Optional.empty();
    }

    public Optional<TimerEvent> findTimerEvent(InternalEvent event) {
        String eventId = event.getId();

        // first try: search be id
        for (TimerEvent timerEvent : timerEvents) {
            if (eventId.equals(timerEvent.getName())) {
                return Optional.of(timerEvent);
            }
        }

        // second try: search by pattern match
        for (TimerEvent timerEvent : timerEvents) {
            Pattern pattern = timerEvent.getPattern();
            if (pattern != null && pattern.matcher(eventId).matches()) {
                return Optional.of(timerEvent);
            }
        }

        // not found
        return Optional.empty();
    }

    public Optional<CancelEvent> findCancelEvent(InternalEvent event) {
        String eventId = event.getId();

        for (CancelEvent cancelEvent : cancelEvents) {
            if (eventId.equals(cancelEvent.getName())) {
                return Optional.of(cancelEvent);
            }
        }

        return Optional.empty();
    }

    public Optional<? extends EventDescriptor> findEventDescriptor(Session session, InternalEvent event) {
        switch (event.getType()) {
            case TIMER:
                return findTimerEvent(event);
            case FILE:
                return findFileEvent(session, event);
            case SESSION:
                return findSessionEvent(event);
            case CANCEL:
                return findCancelEvent(event);
            case MESSAGE:
                return findMessageEvent(event);
            default:
                return Optional.empty();
        }
    }
}
