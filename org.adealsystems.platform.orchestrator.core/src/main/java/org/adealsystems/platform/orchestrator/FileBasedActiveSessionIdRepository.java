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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileBasedActiveSessionIdRepository implements ActiveSessionIdRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedActiveSessionIdRepository.class);

    private static final Pattern ACTIVE_FILE_PATTERN = Pattern.compile('(' + InstanceId.PATTERN_STRING + ")\\.json");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ReentrantLock lock = new ReentrantLock();

    private final SessionIdGenerator sessionIdGenerator;

    private final File baseDirectory;

    public FileBasedActiveSessionIdRepository(SessionIdGenerator sessionIdGenerator, File baseDirectory) {
        this.sessionIdGenerator = Objects.requireNonNull(sessionIdGenerator, "sessionIdGenerator must not be null!");

        this.baseDirectory = Objects.requireNonNull(baseDirectory, "baseDirectory must not be null!");
        if (!baseDirectory.exists()) {
            throw new IllegalArgumentException("Missing mandatory baseDirectory: '" + baseDirectory + "'!");
        }
        if (!baseDirectory.isDirectory()) {
            throw new IllegalArgumentException("baseDirectory '" + baseDirectory + "' must be directory!");
        }
    }

    @Override
    public Collection<InstanceId> listAllActiveInstances() {
        lock.lock();
        try {
            File[] allFiles = baseDirectory.listFiles();
            if (allFiles == null) {
                return Collections.emptySet();
            }

            return Arrays.stream(allFiles)
                .map(file -> {
                    Matcher matcher = ACTIVE_FILE_PATTERN.matcher(file.getName());
                    if (!matcher.matches()) {
                        return null;
                    }
                    String currentId = matcher.group(1);
                    return new InstanceId(currentId);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<SessionId> createActiveSessionId(InstanceId id) {
        File instanceFile = getInstanceFile(id);

        lock.lock();
        try {
            if (instanceFile.exists()) {
                LOGGER.debug("SessionId file {} already exists!", instanceFile);
                return Optional.empty();
            }

            SessionId sessionId = sessionIdGenerator.generate();
            writeSessionId(instanceFile, sessionId);

            return Optional.of(sessionId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<SessionId> retrieveActiveSessionId(InstanceId id) {
        File instanceFile = getInstanceFile(id);

        lock.lock();
        try {
            if (!instanceFile.exists()) {
                return Optional.empty();
            }

            return Optional.of(readSessionId(instanceFile));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SessionId retrieveOrCreateActiveSessionId(InstanceId id) {
        File instanceFile = getInstanceFile(id);

        lock.lock();
        try {
            if (instanceFile.exists()) {
                return readSessionId(instanceFile);
            }

            SessionId sessionId = sessionIdGenerator.generate();
            writeSessionId(instanceFile, sessionId);

            return sessionId;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean deleteActiveSessionId(InstanceId id) {
        File instanceFile = getInstanceFile(id);

        lock.lock();
        try {
            return instanceFile.delete();
        } finally {
            lock.unlock();
        }
    }

    File getInstanceFile(InstanceId id) {
        Objects.requireNonNull(id, "id must not be null!");
        return new File(baseDirectory, id.getId() + ".json");
    }

    private SessionId readSessionId(File instanceFile) {
        try {
            return OBJECT_MAPPER.readValue(instanceFile, SessionIdHolder.class).getSessionId();
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to read instance file '" + instanceFile + "'!", ex);
        }
    }

    private void writeSessionId(File instanceFile, SessionId sessionId) {
        try {
            SessionIdHolder sessionIdHolder = new SessionIdHolder(sessionId);
            OBJECT_MAPPER.writeValue(instanceFile, sessionIdHolder);
            SessionIdHolder reloadedIdHolder = OBJECT_MAPPER.readValue(instanceFile, SessionIdHolder.class);
            if (!sessionIdHolder.equals(reloadedIdHolder)) {
                throw new IllegalStateException("Error verifying active session id file!" +
                    " Stored content " + sessionIdHolder + " is not equal to reloaded content " + reloadedIdHolder);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to write instance file '" + instanceFile + "'!", ex);
        }
    }

    static class SessionIdHolder {
        private final SessionId sessionId;

        SessionIdHolder(@JsonProperty("sessionId") SessionId sessionId) {
            this.sessionId = Objects.requireNonNull(sessionId, "sessionId must not be null!");
        }

        public SessionId getSessionId() {
            return sessionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SessionIdHolder)) return false;
            SessionIdHolder that = (SessionIdHolder) o;
            return Objects.equals(sessionId, that.sessionId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sessionId);
        }

        @Override
        public String toString() {
            return "SessionIdHolder{" +
                "sessionId=" + sessionId +
                '}';
        }
    }
}
