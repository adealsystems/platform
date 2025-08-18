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
import org.adealsystems.platform.io.json.JsonlDrain;
import org.adealsystems.platform.orchestrator.session.SessionUpdateOperation;
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
        lock.lock();
        try (Drain<HistoryEntry<T>> drain = createDrain(id)) {
            HistoryEntry<T> entry = new HistoryEntry<>();
            entry.setTimestamp(timestampFactory.createTimestamp());
            entry.setOperation(operation);
            drain.add(entry);
            LOGGER.debug("Added session operation to history of {}: {}", id, operation);
        } catch (Exception ex) {
            LOGGER.error("Exception while draining session update for {}!", id, ex);
        }
        finally {
            lock.unlock();
        }
    }

    private <T extends SessionUpdateOperation> Drain<HistoryEntry<T>> createDrain(SessionId sessionId) throws IOException {
        File file = createFile(sessionId);
        LOGGER.debug("Creating file drain '{}' for {}.", file, sessionId);
        return createDrain(file, objectMapper);
    }

    private static <T extends SessionUpdateOperation> Drain<HistoryEntry<T>> createDrain(File file, ObjectMapper objectMapper) throws IOException {
        return new JsonlDrain<>(Files.newOutputStream(file.toPath(), CREATE, APPEND), objectMapper);
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

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            HistoryEntry<?> that = (HistoryEntry<?>) o;
            return Objects.equals(timestamp, that.timestamp) && Objects.equals(operation, that.operation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, operation);
        }

        @Override
        public String toString() {
            return "HistoryEntry{" +
                "timestamp=" + timestamp +
                ", operation=" + operation +
                '}';
        }
    }
}
