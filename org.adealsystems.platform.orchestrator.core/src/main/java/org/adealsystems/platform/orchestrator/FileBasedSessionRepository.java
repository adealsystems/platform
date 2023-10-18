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
import org.adealsystems.platform.orchestrator.status.mapping.SessionProcessingStateModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileBasedSessionRepository implements SessionRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSessionRepository.class);

    private static final ObjectMapper OBJECT_MAPPER;
    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        OBJECT_MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.registerModule(new SessionProcessingStateModule());
    }

    private static final Pattern FILE_PATTERN = Pattern.compile('(' + SessionId.PATTERN_STRING + ")\\.json");

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final InstanceId instanceId;

    private final File baseDirectory;

    public FileBasedSessionRepository(InstanceId instanceId, File baseDirectory) {
        Objects.requireNonNull(instanceId, "instanceId must not be null!");
        Objects.requireNonNull(baseDirectory, "baseDirectory must not be null!");
        if (!baseDirectory.exists()) {
            throw new IllegalArgumentException("Missing mandatory baseDirectory: '" + baseDirectory.getAbsolutePath() + "'!");
        }
        if (!baseDirectory.isDirectory()) {
            throw new IllegalArgumentException("baseDirectory '" + baseDirectory.getAbsolutePath() + "' must be directory!");
        }
        this.instanceId = instanceId;
        this.baseDirectory = baseDirectory;
    }

    @Override
    public Set<SessionId> retrieveSessionIds() {
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            File[] allFiles = baseDirectory.listFiles();
            if (allFiles == null) {
                return Collections.emptySet();
            }

            return Arrays.stream(allFiles)
                .map(file -> {
                    Matcher matcher = FILE_PATTERN.matcher(file.getName());
                    if (!matcher.matches()) {
                        return null;
                    }
                    return new SessionId(matcher.group(1));
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public Session createSession(SessionId id) {
        File sessionFile = getSessionFile(id);

        Session session = new Session(instanceId, id);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (sessionFile.exists()) {
                throw new IllegalArgumentException("Session with id '" + id + "' already exists!");
            }

            writeSession(sessionFile, session);
        } finally {
            writeLock.unlock();
        }

        return session;
    }

    @Override
    public Optional<Session> retrieveSession(SessionId id) {
        File sessionFile = getSessionFile(id);

        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            if (!sessionFile.exists()) {
                return Optional.empty();
            }

            return Optional.of(readSession(sessionFile));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Session retrieveOrCreateSession(SessionId id) {
        File sessionFile = getSessionFile(id);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (sessionFile.exists()) {
                return readSession(sessionFile);
            }

            Session session = new Session(instanceId, id);
            writeSession(sessionFile, session);
            return session;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void updateSession(Session session) {
        Objects.requireNonNull(session, "session must not be null!");

        if (!instanceId.equals(session.getInstanceId())) {
            throw new IllegalArgumentException("InstanceId does not match '" + instanceId + "'!");
        }

        SessionId id = session.getId();
        File sessionFile = getSessionFile(id);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (!sessionFile.exists()) {
                throw new IllegalArgumentException("Session with id '" + id + "' does not exist!");
            }

            writeSession(sessionFile, session);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean deleteSession(SessionId id) {
        File sessionFile = getSessionFile(id);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            return sessionFile.delete();
        }
        finally {
            writeLock.unlock();
        }
    }

    File getSessionFile(SessionId id) {
        Objects.requireNonNull(id, "id must not be null!");
        return new File(baseDirectory, id.getId() + ".json");
    }

    private Session readSession(File sessionFile) {
        try {
            Session session = OBJECT_MAPPER.readValue(sessionFile, Session.class);
            if (instanceId.equals(session.getInstanceId())) {
                return session;
            }

            LOGGER.warn("Correcting InstanceId of {} to {}", session, instanceId);
            Session newSession = new Session(instanceId, session.getId(), session.getCreationTimestamp(), session.getInstanceConfiguration());
            newSession.setState(session.getState());
            return newSession;
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to read session file '" + sessionFile.getAbsolutePath() + "'!", ex);
        }
    }

    private void writeSession(File sessionFile, Session session) {
        try {
            OBJECT_MAPPER.writeValue(sessionFile, session);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to write session file '" + sessionFile.getAbsolutePath() + "'!", ex);
        }
    }
}
