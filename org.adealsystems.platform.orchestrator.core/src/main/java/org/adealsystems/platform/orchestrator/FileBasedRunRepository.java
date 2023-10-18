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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileBasedRunRepository implements RunRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedRunRepository.class);

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        OBJECT_MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
    }

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final File baseDirectory;

    public FileBasedRunRepository(File baseDirectory) {
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
    public void createRun(String id) {
        File runFile = getRunFile();

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            CurrentRunState state = null;
            if (runFile.exists()) {
                state = readCurrentRunState(runFile);
            }

            if (state == null) {
                state = new CurrentRunState();
            }

            state.activeRun = id;
            state.activeRunTimestamp = LocalDateTime.now(ZoneId.systemDefault());

            if (CurrentRunState.NOT_AVAILABLE.equals(state.waitingRun)) {
                state.waitingRun = state.activeRun;
                state.waitingRunTimestamp = state.activeRunTimestamp;
            }

            writeCurrentRunState(runFile, state);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Optional<String> retrieveActiveRun() {
        File runFile = getRunFile();

        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            if (!runFile.exists()) {
                return Optional.empty();
            }

            CurrentRunState state = readCurrentRunState(runFile);
            if (state == null || CurrentRunState.NOT_AVAILABLE.equals(state.activeRun)) {
                return Optional.empty();
            }

            return Optional.ofNullable(state.activeRun);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<String> retrieveWaitingRun() {
        File runFile = getRunFile();

        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            if (!runFile.exists()) {
                return Optional.empty();
            }

            CurrentRunState state = readCurrentRunState(runFile);
            if (state == null || CurrentRunState.NOT_AVAILABLE.equals(state.waitingRun)) {
                return Optional.empty();
            }

            return Optional.ofNullable(state.waitingRun);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<LocalDateTime> retrieveActiveRunStartTimestamp() {
        File runFile = getRunFile();

        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            if (!runFile.exists()) {
                return Optional.empty();
            }

            CurrentRunState state = readCurrentRunState(runFile);
            if (state == null || CurrentRunState.NOT_AVAILABLE.equals(state.activeRun)) {
                return Optional.empty();
            }

            return Optional.ofNullable(state.activeRunTimestamp);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<LocalDateTime> retrieveWaitingRunStartTimestamp() {
        File runFile = getRunFile();

        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            if (!runFile.exists()) {
                return Optional.empty();
            }

            CurrentRunState state = readCurrentRunState(runFile);
            if (state == null || CurrentRunState.NOT_AVAILABLE.equals(state.waitingRun)) {
                return Optional.empty();
            }

            return Optional.ofNullable(state.waitingRunTimestamp);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void completeRun() {
        File runFile = getRunFile();

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (!runFile.exists()) {
                throw new IllegalStateException("Unable to complete run! No current run status file available!");
            }

            CurrentRunState state = readCurrentRunState(runFile);
            if (state.waitingRun.equals(state.activeRun)) {
                LOGGER.debug("waitingRun is equal to activeRun");
                state.waitingRun = CurrentRunState.NOT_AVAILABLE;
            }
            else {
                LOGGER.debug("waitingRun is not equal to activeRun");
                state.waitingRun = state.activeRun;
            }
            state.waitingRunTimestamp = LocalDateTime.now(ZoneId.systemDefault());
            writeCurrentRunState(runFile, state);
        } finally {
            writeLock.unlock();
        }
    }

    File getRunFile() {
        return new File(baseDirectory, "current_run.json");
    }

    private CurrentRunState readCurrentRunState(File runFile) {
        try {
            return OBJECT_MAPPER.readValue(runFile, CurrentRunState.class);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to read current run state file '" + runFile.getAbsolutePath() + "'!", ex);
        }
    }

    private void writeCurrentRunState(File runFile, CurrentRunState state) {
        try {
            OBJECT_MAPPER.writeValue(runFile, state);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to write current run state file '" + runFile.getAbsolutePath() + "'!", ex);
        }
    }

    static class CurrentRunState {
        static final String NOT_AVAILABLE = "N/A";

        String activeRun = NOT_AVAILABLE;
        String waitingRun = NOT_AVAILABLE;
        LocalDateTime activeRunTimestamp;
        LocalDateTime waitingRunTimestamp;


        public String getActiveRun() {
            return activeRun;
        }

        public void setActiveRun(String activeRun) {
            this.activeRun = activeRun;
        }

        public String getWaitingRun() {
            return waitingRun;
        }

        public void setWaitingRun(String waitingRun) {
            this.waitingRun = waitingRun;
        }

        public LocalDateTime getActiveRunTimestamp() {
            return activeRunTimestamp;
        }

        public void setActiveRunTimestamp(LocalDateTime activeRunTimestamp) {
            this.activeRunTimestamp = activeRunTimestamp;
        }

        public LocalDateTime getWaitingRunTimestamp() {
            return waitingRunTimestamp;
        }

        public void setWaitingRunTimestamp(LocalDateTime waitingRunTimestamp) {
            this.waitingRunTimestamp = waitingRunTimestamp;
        }
    }
}
