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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.adealsystems.platform.orchestrator.status.mapping.SessionProcessingStateModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileBasedSessionRepository implements SessionRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSessionRepository.class);

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(
        "yyyyMMdd",
        Locale.ROOT
    );
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern(
        "yyyyMMddHHmmss",
        Locale.ROOT
    );
    private static final DateTimeFormatter SESSION_UPDATE_FORMATTER = DateTimeFormatter.ofPattern(
        "HH:mm:ss.SSS",
        Locale.ROOT
    );

    private static final ObjectMapper OBJECT_MAPPER;
    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        OBJECT_MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.registerModule(new SessionProcessingStateModule());
    }

    private static final Pattern FILE_PATTERN
        = Pattern.compile("(?<timestamp>[0-9]{14}_)?(?<id>" + SessionId.PATTERN_STRING + ")\\.json");

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ConcurrentMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<>();
    private final InstanceId instanceId;
    private final File baseDirectory;

    public FileBasedSessionRepository(InstanceId instanceId, File baseDirectory) {
        Objects.requireNonNull(instanceId, "instanceId must not be null!");
        Objects.requireNonNull(baseDirectory, "baseDirectory must not be null!");
        if (!baseDirectory.exists()) {
            throw new IllegalArgumentException("Missing mandatory baseDirectory: '" + baseDirectory + "'!");
        }
        if (!baseDirectory.isDirectory()) {
            throw new IllegalArgumentException("baseDirectory '" + baseDirectory + "' must be directory!");
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
                    return new SessionId(matcher.group("id"));
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<SessionId> retrieveSessionIds(LocalDate createdOn) {
        Objects.requireNonNull(createdOn, "createdOn must not be null!");

        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            File[] allFiles = baseDirectory.listFiles();
            if (allFiles == null) {
                return Collections.emptySet();
            }

            String relevantDate = DATE_FORMATTER.format(createdOn);
            return Arrays.stream(allFiles)
                .map(file -> {
                    Matcher matcher = FILE_PATTERN.matcher(file.getName());
                    if (!matcher.matches()) {
                        return null;
                    }

                    SessionId id = new SessionId(matcher.group("id"));

                    String ts = matcher.group("timestamp");
                    if (ts != null) {
                        return ts.startsWith(relevantDate) ? id : null;
                    }

                    // fallback
                    Optional<Session> session = retrieveSession(id);
                    if (session.isEmpty()) {
                        return null;
                    }
                    LocalDateTime sessionTimestamp = session.get().getCreationTimestamp();
                    if (sessionTimestamp == null) {
                        return null;
                    }
                    return createdOn.equals(sessionTimestamp.toLocalDate()) ? id : null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public Session createSession(SessionId sessionId) {
        File sessionFile = getSessionFile(sessionId);
        if (sessionFile.exists()) {
            throw new IllegalArgumentException("Session with id '" + sessionId + "' already exists!");
        }

        Session session = new Session(instanceId, sessionId);

        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        lock.lock();
        try {
            writeSession(sessionFile, session);
        }
        finally {
            lock.unlock();
        }

        return session;
    }

    @Override
    public Optional<Session> retrieveSession(SessionId id) {
        File sessionFile = findOrCreateSessionFile(id);

        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            if (!sessionFile.exists()) {
                return Optional.empty();
            }

            return Optional.of(readSession(sessionFile));
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public Session retrieveOrCreateSession(SessionId sessionId) {
        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        lock.lock();
        try {
            return internalRetrieveOrCreateSession(sessionId);
        }
        finally {
            lock.unlock();
        }
    }

    private Session internalRetrieveOrCreateSession(SessionId id) {
        File sessionFile = findOrCreateSessionFile(id);
        if (sessionFile.exists()) {
            return readSession(sessionFile);
        }

        Session session = new Session(instanceId, id);
        writeSession(sessionFile, session);
        return session;
    }

    @Override
    public void updateSession(Session session) {
        Objects.requireNonNull(session, "session must not be null!");

        SessionId sessionId = session.getId();
        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        lock.lock();
        try {
            internalUpdateSession(session);
        }
        finally {
            lock.unlock();
        }
    }

    private void internalUpdateSession(Session session) {
        if (!instanceId.equals(session.getInstanceId())) {
            throw new IllegalArgumentException("InstanceId does not match '" + instanceId + "'!");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Updating session {}", session);

            // log the update caller
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            String history = session.getStateValue(Session.UPDATE_HISTORY).orElse("");
            if (!history.isEmpty()) {
                history += ", ";
            }

            String me = this.getClass().getName();
            String timestamp = SESSION_UPDATE_FORMATTER.format(LocalDateTime.now(ZoneId.systemDefault()));
            boolean found = false;
            for (int i = 0; i < stackTrace.length; i++) {
                if (i > 10 && !found) {
                    // Strange! Caller class should be in one of the first 3-4 elements.
                    LOGGER.warn("Unable to identify caller from {}", Arrays.asList(stackTrace));
                    break;
                }

                if (i == 0) {
                    continue;
                }

                StackTraceElement element = stackTrace[i];
                String value = element.toString();
                LOGGER.trace("\tTRACE[{}]: {}", i, value);

                if (!found && !value.startsWith(me)) {
                    found = true;
                    history += timestamp + ": " + value;
                    session.setStateValue(Session.UPDATE_HISTORY, history);
                }
            }
        }

        SessionId id = session.getId();
        File sessionFile = findOrCreateSessionFile(id);
        if (!sessionFile.exists()) {
            throw new IllegalArgumentException("Session '" + id + "' does not exist!");
        }

        writeSession(sessionFile, session);
    }

    @Override
    public boolean deleteSession(SessionId sessionId) {
        File sessionFile = findOrCreateSessionFile(sessionId);

        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        lock.lock();
        try {
            return sessionFile.delete();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public Session modifySession(SessionId sessionId, Consumer<Session> modifier) {
        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        lock.lock();
        try {
            Session session = internalRetrieveOrCreateSession(sessionId);
            modifier.accept(session);
            internalUpdateSession(session);

            return session.clone();
        }
        finally {
            lock.unlock();
        }
    }

    File getSessionFile(SessionId id) {
        Objects.requireNonNull(id, "id must not be null!");
        LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
        String timestamp = TIMESTAMP_FORMATTER.format(now);
        return new File(baseDirectory, timestamp + '_' + id.getId() + ".json");
    }

    File findSessionFile(SessionId id) {
        Objects.requireNonNull(id, "id must not be null!");
        String fileSuffix = id.getId() + ".json";

        File[] matchingFiles = baseDirectory.listFiles((dir, name) -> name.endsWith(fileSuffix));
        if (matchingFiles == null || matchingFiles.length == 0) {
            return null;
        }
        if (matchingFiles.length > 1) {
            LOGGER.warn("Multiple matching files found for session id '{}'", id);
        }

        return matchingFiles[0];
    }

    File findOrCreateSessionFile(SessionId id) {
        File file = findSessionFile(id);
        if (file == null) {
            file = getSessionFile(id);
        }
        return file;
    }

    private Session readSession(File sessionFile) {
        try {
            Session session = OBJECT_MAPPER.readValue(sessionFile, Session.class);
            if (instanceId.equals(session.getInstanceId())) {
                return session;
            }

            LOGGER.warn("Correcting InstanceId of {} to {}", session, instanceId);
            Session newSession = new Session(
                instanceId,
                session.getId(),
                session.getCreationTimestamp(),
                session.getInstanceConfiguration()
            );
            newSession.setState(session.getState());
            newSession.setProcessingState(session.getProcessingState());
            return newSession;
        }
        catch (IOException ex) {
            throw new IllegalStateException("Unable to read session file '" + sessionFile + "'!", ex);
        }
    }

    private void writeSession(File sessionFile, Session session) {
        try {
            OBJECT_MAPPER.writeValue(sessionFile, session);
        }
        catch (IOException ex) {
            throw new IllegalStateException("Unable to write session file '" + sessionFile + "'!", ex);
        }
    }
}
