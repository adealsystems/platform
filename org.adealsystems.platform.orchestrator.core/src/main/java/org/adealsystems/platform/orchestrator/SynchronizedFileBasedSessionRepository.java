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
import org.adealsystems.platform.orchestrator.session.SessionUpdateMessageOperation;
import org.adealsystems.platform.orchestrator.session.SessionUpdateOperation;
import org.adealsystems.platform.orchestrator.session.SessionUpdateOperationModule;
import org.adealsystems.platform.orchestrator.status.mapping.SessionProcessingStateModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SynchronizedFileBasedSessionRepository implements SessionRepository, ReentrantLockAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedFileBasedSessionRepository.class);

    private static final DateTimeFormatter DATE_FORMATTER
        = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ROOT);
    private static final DateTimeFormatter TIMESTAMP_FORMATTER
        = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.ROOT);
    private static final DateTimeFormatter SESSION_UPDATE_FORMATTER
        = DateTimeFormatter.ofPattern("HH:mm:ss.SSS", Locale.ROOT);

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        OBJECT_MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.registerModule(new SessionProcessingStateModule());
        OBJECT_MAPPER.registerModule(new SessionUpdateOperationModule());
    }

    private static final Pattern FILE_PATTERN
        = Pattern.compile("(?<timestamp>[0-9]{14}_)?(?<id>" + SessionId.PATTERN_STRING + ")\\.json");

    private final ConcurrentMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<>();
    private final InstanceId instanceId;
    private final File baseDirectory;

    public SynchronizedFileBasedSessionRepository(InstanceId instanceId, File baseDirectory) {
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
    public Map<String, ReentrantLock> getLocks() {
        return lockMap;
    }

    @Override
    public Set<SessionId> retrieveSessionIds() {
        ReentrantLock lock = lockMap.computeIfAbsent("session-ids", id -> new ReentrantLock());
        if (lock.isLocked()) {
            LOGGER.info("session-ids lock is already locked, waiting ...");
        }

        lock.lock();
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
            lock.unlock();
        }
    }

    @Override
    public Set<SessionId> retrieveSessionIds(LocalDate createdOn) {
        Objects.requireNonNull(createdOn, "createdOn must not be null!");

        ReentrantLock lock = lockMap.computeIfAbsent("session-ids", id -> new ReentrantLock());
        if (lock.isLocked()) {
            LOGGER.info("session-ids lock is already locked, waiting ...");
        }

        lock.lock();
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
            lock.unlock();
        }
    }

    @Override
    public Session createSession(SessionId sessionId) {
        Session session = new Session(instanceId, sessionId);
        return internalCreateSession(session);
    }

    @Override
    public Session createSession(SessionId sessionId, LocalDateTime createdOn, Map<String, String> config) {
        Session session = new Session(instanceId, sessionId, createdOn, config, new Session.SessionUpdates());
        return internalCreateSession(session);
    }

    private Session internalCreateSession(Session session) {
        SessionId sessionId = session.getId();

        File sessionFile = getSessionFile(sessionId);
        if (sessionFile.exists()) {
            throw new IllegalArgumentException("Session with id '" + sessionId + "' already exists!");
        }

        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        if (lock.isLocked()) {
            LOGGER.info("Session with id '{}' already locked, waiting (createSession) ...", sessionId);
        }

        lock.lock();
        LOGGER.info("Locked '{}' (createSession)", sessionId);
        try {
            writeSession(sessionFile, session);
        }
        finally {
            lock.unlock();
            LOGGER.info("Unlocked '{}' (createSession)", sessionId);
        }

        return session;
    }

    @Override
    public Optional<Session> retrieveSession(SessionId sessionId) {
        File sessionFile = findOrCreateSessionFile(sessionId);

        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        if (lock.isLocked()) {
            LOGGER.info("Session with id '{}' already locked, waiting (retrieveSession) ...", sessionId);
        }

        lock.lock();
        LOGGER.info("Locked '{}' (retrieveSession)", sessionId);
        try {
            if (!sessionFile.exists()) {
                return Optional.empty();
            }

            return Optional.of(readSession(sessionFile));
        }
        finally {
            lock.unlock();
            LOGGER.info("Unlocked '{}' (retrieveSession)", sessionId);
        }
    }

    @Override
    public Session retrieveOrCreateSession(SessionId sessionId) {
        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        if (lock.isLocked()) {
            LOGGER.info("Session with id '{}' already locked, waiting (retrieveOrCreateSession) ...", sessionId);
        }

        lock.lock();
        LOGGER.info("Locked '{}' (retrieveOrCreateSession)", sessionId);
        try {
            return internalRetrieveOrCreateSession(sessionId);
        }
        finally {
            lock.unlock();
            LOGGER.info("Unlocked '{}' (retrieveOrCreateSession)", sessionId);
        }
    }

    private Session internalRetrieveOrCreateSession(SessionId sessionId) {
        File sessionFile = findOrCreateSessionFile(sessionId);
        if (sessionFile.exists()) {
            return readSession(sessionFile);
        }

        LOGGER.debug("Creating new session for {} with id '{}'", instanceId, sessionId);
        Session session = new Session(instanceId, sessionId);
        writeSession(sessionFile, session);
        return session;
    }

    @Override
    public Session updateSession(Session session) {
        Objects.requireNonNull(session, "session must not be null!");

        SessionId sessionId = session.getId();
        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        if (lock.isLocked()) {
            LOGGER.info("Session with id '{}' already locked, waiting (updateSession) ...", sessionId);
        }

        lock.lock();
        LOGGER.info("Locked '{}' (updateSession)", sessionId);
        try {
            Session currentActiveSession = retrieveSession(sessionId).orElseThrow(IllegalStateException::new);

            String updateVersionChecksum = session.getChecksum();
            if (!updateVersionChecksum.equals(currentActiveSession.getChecksum())) {
                LOGGER.warn(
                    "Detected a concurrent session modification on update between {} and {}",
                    updateVersionChecksum,
                    currentActiveSession.getChecksum()
                );
                session = internalMergeActiveSession(session, currentActiveSession);
                session.extendStateRegistry("merged", String.valueOf(LocalDateTime.now(ZoneId.systemDefault())));
            }

            // re-calculate the checksum
            session.updateChecksum();

            internalUpdateSession(session);
            return session;
        }
        finally {
            lock.unlock();
            LOGGER.info("Unlocked '{}' (updateSession)", sessionId);
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
            String myPackage = me.getClass().getPackage().getName();
            String threadName = Thread.currentThread().getName();
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
                    if (value.startsWith(myPackage)) {
                        value = value.substring(myPackage.length());
                    }
                    history += '[' + threadName + "] " + timestamp + ": " + value;
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
        if (lock.isLocked()) {
            LOGGER.info("Session with id '{}' already locked, waiting (deleteSession) ...", sessionId);
        }

        lock.lock();
        LOGGER.info("Locked '{}' (deleteSession)", sessionId);
        try {
            return sessionFile.delete();
        }
        finally {
            lock.unlock();
            LOGGER.info("Unlocked '{}' (deleteSession)", sessionId);
        }
    }

    @Override
    public Session modifySession(SessionId sessionId, Consumer<Session> modifier) {
        Session session = internalRetrieveOrCreateSession(sessionId);

        modifier.accept(session);

        ReentrantLock lock = lockMap.computeIfAbsent(sessionId.getId(), id -> new ReentrantLock());
        if (lock.isLocked()) {
            LOGGER.info("Session with id '{}' already locked, waiting (modifySession) ...", sessionId);
        }

        lock.lock();
        LOGGER.info("Locked '{}' (modifySession)", sessionId);
        try {
            Session currentActiveSession = internalRetrieveOrCreateSession(sessionId);

            String updateVersionChecksum = session.getChecksum();
            if (!updateVersionChecksum.equals(currentActiveSession.getChecksum())) {
                LOGGER.info(
                    "Detected a concurrent session modification for {} between {} and {}",
                    sessionId,
                    updateVersionChecksum,
                    currentActiveSession.getChecksum()
                );
                session = internalMergeActiveSession(session, currentActiveSession);
            }

            // re-calculate the checksum
            session.updateChecksum();

            internalUpdateSession(session);
        }
        finally {
            lock.unlock();
            LOGGER.info("Unlocked '{}' (modifySession)", sessionId);
        }

        return session;
    }

    private Session internalMergeActiveSession(Session session, Session currentActiveSession) {
        Session.SessionUpdates updates = session.getSessionUpdates();
        Session.SessionUpdates baseUpdates = currentActiveSession.getSessionUpdates();
        LOGGER.debug(
            "Found {} updates for the new and {} - for the base session, searching for differences",
            updates,
            baseUpdates
        );

        Session merged = Session.copyOf(session);
        Session.SessionUpdates mergedUpdates = applyMissingUpdates(merged, updates, baseUpdates);

        // set full updates sequence to the session
        merged.setSessionUpdates(mergedUpdates);

        return merged;
    }

    private Session.SessionUpdates applyMissingUpdates(
        Session session,
        Session.SessionUpdates updates,
        Session.SessionUpdates baseUpdates
    ) {
        List<SessionUpdateOperation> result = new ArrayList<>();
        List<SessionUpdateOperation> baseUpdateOperations = baseUpdates.getUpdates();
        List<SessionUpdateOperation> newUpdateOperations = updates.getUpdates();
        int lastSharedIndex = 0;
        for (int i = 0; i < baseUpdateOperations.size(); i++) {
            SessionUpdateOperation baseOp = baseUpdateOperations.get(i);
            if (i >= newUpdateOperations.size()) {
                lastSharedIndex = i;
                break;
            }

            SessionUpdateOperation newOp = newUpdateOperations.get(i);
            if (!baseOp.equals(newOp)) {
                lastSharedIndex = i;
                break;
            }

            result.add(baseOp);
        }

        // cluster update operations by its types
        Map<Class<? extends SessionUpdateOperation>, Set<SessionUpdateOperation>> ops = new HashMap<>();
        clusterUpdates(baseUpdateOperations, lastSharedIndex, ops);
        clusterUpdates(newUpdateOperations, lastSharedIndex, ops);

        // merge clustered updates
        List<SessionUpdateOperation> mergedOps = new ArrayList<>();
        for (Map.Entry<Class<? extends SessionUpdateOperation>, Set<SessionUpdateOperation>> entry : ops.entrySet()) {
            Class<? extends SessionUpdateOperation> type = entry.getKey();
            Set<SessionUpdateOperation> typeUpdates = entry.getValue();
            if (SessionUpdateMessageOperation.class.isAssignableFrom(type)) {
                if (typeUpdates.size() == 1) {
                    mergedOps.add(typeUpdates.iterator().next());
                    continue;
                }

                StringBuilder message = new StringBuilder("Merged message: "); // NOPMD
                for (SessionUpdateOperation op : typeUpdates) {
                    SessionUpdateMessageOperation messageOp = (SessionUpdateMessageOperation) op;
                    String msg = messageOp.getMessage();
                    message.append(", ").append(msg);
                }
                mergedOps.add(new SessionUpdateMessageOperation(message.toString()));
            }

            // all other types shouldn't be merged, just collect all of them
            mergedOps.addAll(typeUpdates);
        }

        // apply merged updates
        for (SessionUpdateOperation op : mergedOps) {
            op.apply(session);
        }

        result.addAll(mergedOps);

        // build merged session update structure
        Session.SessionUpdates resultUpdates = new Session.SessionUpdates();
        resultUpdates.setUpdates(result);
        return resultUpdates;
    }

    private void clusterUpdates(
        List<SessionUpdateOperation> updates,
        int startIndex,
        Map<Class<? extends SessionUpdateOperation>, Set<SessionUpdateOperation>> clusters
    ) {
        for (int i = startIndex; i < updates.size(); i++) {
            SessionUpdateOperation op = updates.get(i);
            Class<? extends SessionUpdateOperation> opClass = op.getClass();
            Set<SessionUpdateOperation> sameTypeUpdates = clusters.get(opClass);
            if (sameTypeUpdates == null) {
                sameTypeUpdates = new HashSet<>(); // NOPMD
                clusters.put(opClass, sameTypeUpdates);
            }
            sameTypeUpdates.add(op);
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
                LOGGER.debug("Returning original loaded session for {}", instanceId);
                return session;
            }

            LOGGER.warn("Correcting InstanceId of {} to {}", session.getId(), instanceId);
            Session newSession = new Session(
                instanceId,
                session.getId(),
                session.getCreationTimestamp(),
                session.getInstanceConfiguration(),
                session.getSessionUpdates()
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
