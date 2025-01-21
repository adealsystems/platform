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

package org.adealsystems.platform.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.json.JsonlDrain;
import org.adealsystems.platform.io.json.JsonlWell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.adealsystems.platform.orchestrator.InternalEvent.ATTR_RUN_ID;
import static org.adealsystems.platform.orchestrator.InternalEvent.normalizeDynamicContent;

public class FileBasedEventHistory implements EventHistory, OrphanEventSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedEventHistory.class);

    public static final String RAW_FILE_NAME = "_raw";
    public static final String ARCHIVE_DIRECTORY_NAME = "_archive";
    public static final String ORPHAN_FILE_NAME = "_orphan";
    public static final String MONITORING_DIRECTORY_NAME = "_monitoring";
    public static final String FILE_EXTENSION = ".jsonl";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssSSS", Locale.ROOT);

    private final ReentrantLock lock = new ReentrantLock();
    private final File baseDirectory;
    private final TimestampFactory timestampFactory;
    private final RunRepository runRepository;
    private final ObjectMapper objectMapper;

    public FileBasedEventHistory(File baseDirectory, RunRepository runRepository, ObjectMapper objectMapper) {
        this(baseDirectory, new SystemTimestampFactory(), runRepository, objectMapper);
    }

    public FileBasedEventHistory(
        File baseDirectory,
        TimestampFactory timestampFactory,
        RunRepository runRepository,
        ObjectMapper objectMapper
    ) {
        this.timestampFactory = Objects.requireNonNull(timestampFactory, "timestampFactory must not be null!");
        this.runRepository = Objects.requireNonNull(runRepository, "runRepository must not be null!");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null!");

        Objects.requireNonNull(baseDirectory, "baseDirectory must not be null!");
        if (!baseDirectory.exists()) {
            throw new IllegalArgumentException("Missing mandatory baseDirectory: '" + baseDirectory + "'!");
        }
        if (!baseDirectory.isDirectory()) {
            throw new IllegalArgumentException("baseDirectory '" + baseDirectory + "' must be directory!");
        }
        this.baseDirectory = baseDirectory;
    }

    @Override
    public void add(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");
        addAll(Collections.singleton(event));
    }

    @Override
    public void addAll(Iterable<InternalEvent> events) {
        Map<EventAffiliation, List<InternalEvent>> splitEvents = splitEvents(events);

        lock.lock();
        try {
            for (Map.Entry<EventAffiliation, List<InternalEvent>> current : splitEvents.entrySet()) {
                EventAffiliation key = current.getKey();
                try (Drain<InternalEvent> drain = createDrain(key)) {
                    List<InternalEvent> value = current.getValue();
                    drain.addAll(value);
                    LOGGER.debug("Added {} event(s) to history {}.", value.size(), key);
                } catch (Exception ex) {
                    LOGGER.error("Exception while draining events for {}!", key, ex);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean drainOrphanEventsInto(InternalEventClassifier eventClassifier, EventAffiliation idTuple, Drain<InternalEvent> eventDrain) {
        Objects.requireNonNull(idTuple, "idTuple must not be null!");
        Objects.requireNonNull(eventDrain, "eventDrain must not be null!");

        InstanceId instanceId = idTuple.getInstanceId();
        if (instanceId == null) {
            throw new IllegalArgumentException("instanceId of idTuple must not be null! " + idTuple);
        }

        SessionId sessionId = idTuple.getSessionId();
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId of idTuple must not be null! " + idTuple);
        }
        EventAffiliation orphanIdTuple = new EventAffiliation(instanceId, null);

        lock.lock();
        try {
            File orphanFile = createFile(orphanIdTuple);
            if (!orphanFile.exists()) {
                LOGGER.debug("No orphan events found for instanceId {}.", instanceId);
                return false;
            }
            if (!orphanFile.isFile()) {
                LOGGER.warn("{} is not a file! No orphan events transferred for instanceId {}!", orphanFile, instanceId);
                return false;
            }

            int eventCounter = 0;
            try (Well<InternalEvent> well = createWell(orphanFile)) {
                Optional<RunSpecification> oRun = eventClassifier.getCurrentRun();

                for (InternalEvent current : well) {
                    if (current == null) {
                        LOGGER.warn("Ignoring null orphan event for '{}'!", instanceId);
                        continue;
                    }

                    if (oRun.isPresent()) {
                        RunSpecification run = oRun.get();
                        LOGGER.debug("EventClassifier {} is run-specific for {}, checking run validity",
                            eventClassifier.getClass().getName(), run);
                        if (RunSpecification.isEventOutdated(current, run.getType(), runRepository)) {
                            LOGGER.warn("Detected an outdated event {}, current run-id: '{}'", current, run);
                            continue;
                        }

                        current.setAttributeValue(ATTR_RUN_ID, run.getId());
                    } else {
                        LOGGER.debug("Event {} is not run-specific", current);
                    }

                    current.setSessionId(sessionId);
                    eventDrain.add(current);
                    eventCounter++;
                }
            } catch (Exception ex) {
                LOGGER.error("Exception while draining orphan events for '{}'!", instanceId, ex);
            }

            if (orphanFile.delete()) {
                LOGGER.debug("Deleted orphan file '{}'.", orphanFile);
            } else {
                LOGGER.error("Failed to delete orphan file '{}'!", orphanFile);
            }

            return eventCounter > 0;
        } finally {
            lock.unlock();
        }
    }

    public void rollRawEvents() {
        lock.lock();
        try {
            File rawEventsFile = createFile(EventAffiliation.RAW);
            if (!rawEventsFile.isFile()) {
                LOGGER.info("No raw events file '{}' found!", rawEventsFile);
                return;
            }

            File archiveDir = new File(baseDirectory, ARCHIVE_DIRECTORY_NAME);
            if (archiveDir.mkdirs()) {
                LOGGER.info("Created archive directory '{}'", archiveDir);
            } else {
                if (!archiveDir.isDirectory()) {
                    LOGGER.error("Archive dir '{}' is not a directory!", archiveDir);
                    return;
                }
                LOGGER.debug("Using existing archive directory '{}'", archiveDir);
            }

            LocalDateTime timestamp = timestampFactory.createTimestamp();
            String prefix = timestamp.format(FORMATTER);
            File targetFile = new File(archiveDir, prefix + RAW_FILE_NAME + FILE_EXTENSION);

            try {
                Files.move(rawEventsFile.toPath(), targetFile.toPath(), REPLACE_EXISTING);
                LOGGER.info("Moved raw events to '{}'", targetFile);
                return;
            } catch (IOException ex) {
                LOGGER.warn("Failed to move raw events file to '{}'", targetFile, ex);
            }

            try {
                Files.copy(rawEventsFile.toPath(), targetFile.toPath(), REPLACE_EXISTING);
            } catch (IOException ex) {
                LOGGER.error("Failed to copy raw events file to '{}'", targetFile, ex);
                return;
            }

            try {
                Files.delete(rawEventsFile.toPath());
            } catch (IOException ex) {
                LOGGER.error("Failed to delete raw events file", ex);
                return;
            }

            LOGGER.info("Manually moved raw events to '{}'", targetFile);
        } finally {
            lock.unlock();
        }
    }

    private Drain<InternalEvent> createDrain(EventAffiliation eventAffiliation) throws IOException {
        File file = createFile(eventAffiliation);
        LOGGER.debug("Creating file drain '{}' for {}.", file, eventAffiliation);
        return createDrain(file, objectMapper);
    }

    private Well<InternalEvent> createWell(File orphanFile) throws IOException {
        return new JsonlWell<>(InternalEvent.class, Files.newInputStream(orphanFile.toPath()), objectMapper);
    }

    private static Drain<InternalEvent> createDrain(File file, ObjectMapper objectMapper) throws IOException {
        return new JsonlDrain<>(Files.newOutputStream(file.toPath(), CREATE, APPEND), objectMapper);
    }

    File createFile(EventAffiliation key) {
        InstanceId instanceId = key.getInstanceId();
        if (instanceId == null) {
            return new File(baseDirectory, RAW_FILE_NAME + FILE_EXTENSION);
        }

        File instanceBaseDirectory = new File(baseDirectory, instanceId.getId());
        if (!instanceBaseDirectory.mkdirs()) {
            if (!instanceBaseDirectory.isDirectory()) {
                throw new IllegalStateException("Failed to create instanceBaseDirectory '" + instanceBaseDirectory + "'!");
            }
            LOGGER.debug("Using existing instanceBaseDirectory '{}'", instanceBaseDirectory);
        } else {
            LOGGER.debug("Created instanceBaseDirectory '{}'", instanceBaseDirectory);
        }

        SessionId sessionId = key.getSessionId();
        if (sessionId == null) {
            return new File(instanceBaseDirectory, ORPHAN_FILE_NAME + FILE_EXTENSION);
        }

        String id = sessionId.getId();
        if (key.isProcessed()) {
            File monitoringBaseDirectory = new File(instanceBaseDirectory, MONITORING_DIRECTORY_NAME);
            if (!monitoringBaseDirectory.mkdirs()) {
                if (!monitoringBaseDirectory.isDirectory()) {
                    throw new IllegalStateException("Failed to create monitoringBaseDirectory '" + monitoringBaseDirectory + "'!");
                }
                LOGGER.debug("Using existing monitoringBaseDirectory '{}'", monitoringBaseDirectory);
            } else {
                LOGGER.debug("Created monitoringBaseDirectory '{}'", monitoringBaseDirectory);
            }

            return new File(monitoringBaseDirectory, id + "_processed" + FILE_EXTENSION);
        }

        return new File(instanceBaseDirectory, id + FILE_EXTENSION);
    }

    static Map<EventAffiliation, List<InternalEvent>> splitEvents(Iterable<InternalEvent> events) {
        Objects.requireNonNull(events, "events must not be null!");
        Map<EventAffiliation, List<InternalEvent>> result = new HashMap<>();

        for (InternalEvent event : events) {
            if (event == null) {
                LOGGER.warn("Skipping null event entry!");
                continue;
            }

            InternalEvent clonedEvent = event.clone();
            EventAffiliation key = deriveEventAffiliation(clonedEvent);
            if (key.getInstanceId() == null && key.getSessionId() != null) {
                LOGGER.warn("Changing {} to RAW and fixing event {}...", key, clonedEvent);
                key = EventAffiliation.RAW;
                clonedEvent = InternalEvent.deriveUnprocessedInstance(clonedEvent);
                clonedEvent.setSessionId(null);
            }
            List<InternalEvent> value = result.computeIfAbsent(key, k -> new ArrayList<>()); // NOPMD - AvoidInstantiatingObjectsInLoops

            value.add(clonedEvent);
        }

        return result;
    }

    private static EventAffiliation deriveEventAffiliation(InternalEvent event) {
        InstanceId instanceId = event.getInstanceId();
        if (instanceId == null) {
            return new EventAffiliation(null, null, event.isProcessed());
        }

        Optional<String> oDynamicContent = InternalEvent.getDynamicContentAttribute(event);
        InstanceId dynamicId = oDynamicContent
            .map(dynamicContent -> new InstanceId(instanceId.getId() + '-' + normalizeDynamicContent(dynamicContent.toLowerCase(Locale.ROOT))))
            .orElse(instanceId);
        return new EventAffiliation(dynamicId, event.getSessionId(), event.isProcessed());
    }
}
