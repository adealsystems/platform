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

package org.adealsystems.platform.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.json.JsonlDrain;
import org.adealsystems.platform.io.json.JsonlWell;
import org.adealsystems.platform.orchestrator.session.SessionUpdateOperation;
import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;

public class FileBasedSessionUpdateHistory implements SessionUpdateHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSessionUpdateHistory.class);

    public static final String FILE_EXTENSION = ".jsonl";

    private final ReentrantLock lock = new ReentrantLock();
    private final File baseDirectory;
    private final TimestampFactory timestampFactory;
    private final ObjectMapper objectMapper;

    public FileBasedSessionUpdateHistory(File baseDirectory, ObjectMapper objectMapper) {
        this(baseDirectory, new SystemTimestampFactory(), objectMapper);
    }

    public FileBasedSessionUpdateHistory(
        File baseDirectory,
        TimestampFactory timestampFactory,
        ObjectMapper objectMapper
    ) {
        Objects.requireNonNull(baseDirectory, "baseDirectory must not be null!");
        if (!baseDirectory.exists()) {
            throw new IllegalArgumentException("Missing mandatory baseDirectory: '" + baseDirectory + "'!");
        }
        if (!baseDirectory.isDirectory()) {
            throw new IllegalArgumentException("baseDirectory '" + baseDirectory + "' must be directory!");
        }
        this.baseDirectory = baseDirectory;

        this.timestampFactory = timestampFactory;
        this.objectMapper = objectMapper;
    }

    @Override
    public <T extends SessionUpdateOperation> void add(SessionId id, T operation) {
        this.add(id, operation, null);
    }

    @Override
    public <T extends SessionUpdateOperation> void add(SessionId id, T operation, String comment) {
        lock.lock();
        try (Drain<HistoryEntry<T>> drain = createDrain(id)) {
            HistoryEntry<T> entry = new HistoryEntry<>();
            entry.setTimestamp(timestampFactory.createTimestamp());
            entry.setOperation(operation);
            entry.setComment(comment);
            drain.add(entry);
            LOGGER.debug("Added session operation to history of {}: {}", id, operation);
        } catch (Exception ex) {
            LOGGER.error("Exception while draining session update for {}!", id, ex);
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public <T extends SessionUpdateOperation> Session update(Session session) {
        if (session == null) {
            return null;
        }

        SessionId id = session.getId();

        LocalDateTime lastUpdate;
        SessionProcessingState processingState = session.getProcessingState();
        if (processingState == null) {
            lastUpdate = null;
            LOGGER.debug("No session processing state available for session {}", id);
        }
        else {
            lastUpdate = processingState.getLastUpdated();
            LOGGER.debug("Applying session updates for session {} after {}", id, lastUpdate);
        }

        lock.lock();
        try (Well<HistoryEntry<T>> well = createWell(id)){
            for (HistoryEntry<T> entry : well) {
                LocalDateTime timestamp = entry.getTimestamp();
                if (lastUpdate == null || timestamp.isAfter(lastUpdate)) {
                    LOGGER.debug("Applying session update for {}: {}", id, entry);
                    entry.getOperation().apply(session);
                }
                else {
                    LOGGER.debug("Skipping session update for {}: {}", id, entry);
                }
            }
        }
        catch (Exception ex) {
            LOGGER.error("Exception while reading session updates for {}!", id, ex);
        }
        finally {
            lock.unlock();
        }

        return session;
    }

    private <T extends SessionUpdateOperation> Drain<HistoryEntry<T>> createDrain(SessionId sessionId) throws IOException {
        File file = createFile(sessionId);
        LOGGER.debug("Creating file drain '{}' for {}.", file, sessionId);
        return createDrain(file, objectMapper);
    }

    private <T extends SessionUpdateOperation> Drain<HistoryEntry<T>> createDrain(File file, ObjectMapper objectMapper) throws IOException {
        return new JsonlDrain<>(Files.newOutputStream(file.toPath(), CREATE, APPEND), objectMapper);
    }

    private <T extends SessionUpdateOperation> Well<HistoryEntry<T>> createWell(SessionId sessionId) throws IOException {
        File file = createFile(sessionId);
        LOGGER.debug("Creating file well '{}' for {}.", file, sessionId);
        return createWell(file, objectMapper);
    }

    private <T extends SessionUpdateOperation> Well<HistoryEntry<T>> createWell(File file, ObjectMapper objectMapper) throws IOException {
        Class<HistoryEntry<T>> entryClass = (Class<HistoryEntry<T>>) new HistoryEntry<>().getClass();
        return new JsonlWell<>(entryClass, Files.newInputStream(file.toPath(), READ), objectMapper);
    }

    File createFile(SessionId sessionId) {
        String id = sessionId.getId();
        if (!baseDirectory.mkdirs()) {
            if (!baseDirectory.isDirectory()) {
                throw new IllegalStateException("Failed to create sessionBaseDirectory '" + baseDirectory + "'!");
            }
            LOGGER.debug("Using existing sessionBaseDirectory '{}'", baseDirectory);
        } else {
            LOGGER.debug("Created sessionBaseDirectory '{}'", baseDirectory);
        }

        return new File(baseDirectory, id + FILE_EXTENSION);
    }

    public static class HistoryEntry<T extends SessionUpdateOperation> {
        private LocalDateTime timestamp;
        private T operation;
        private String comment;

        public LocalDateTime getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
        }

        public T getOperation() {
            return operation;
        }

        public void setOperation(T operation) {
            this.operation = operation;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            HistoryEntry<?> that = (HistoryEntry<?>) o;
            return Objects.equals(timestamp, that.timestamp)
                && Objects.equals(operation, that.operation)
                && Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, operation, comment);
        }

        @Override
        public String toString() {
            return "HistoryEntry{" +
                "timestamp=" + timestamp +
                ", operation=" + operation +
                ", comment='" + comment + '\'' +
                '}';
        }
    }
}
