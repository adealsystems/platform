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

import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.adealsystems.platform.orchestrator.SessionEventConstants.DYNAMIC_CONTENT_ATTRIBUTE_NAME;


@RestController
@RequestMapping("/status")
public class StatusController {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatusController.class);

    private final InstanceRepository instanceRepository;
    private final SessionRepositoryFactory sessionRepositoryFactory;
    private final ActiveSessionIdRepository activeSessionIdRepository;
    private final OrchestratorRuntime runtime;
    private final BlockingQueueInternalEventQueue rawEventQueue;
    private final Map<InstanceId, BlockingQueueInternalEventQueue> instanceEventQueuesMapping;

    public StatusController(
        InstanceRepository instanceRepository,
        SessionRepositoryFactory sessionRepositoryFactory,
        ActiveSessionIdRepository activeSessionIdRepository,
        OrchestratorRuntime runtime,
        @Qualifier("raw-event-queue") BlockingQueueInternalEventQueue rawEventQueue,
        Map<InstanceId, BlockingQueueInternalEventQueue> instanceEventQueuesMapping
    ) {
        this.instanceRepository = Objects.requireNonNull(instanceRepository, "instanceRepository must not be null!");
        this.sessionRepositoryFactory = Objects.requireNonNull(sessionRepositoryFactory, "sessionRepositoryFactory must not be null!");
        this.activeSessionIdRepository = Objects.requireNonNull(activeSessionIdRepository, "activeSessionIdRepository must not be null!");
        this.runtime = Objects.requireNonNull(runtime, "runtime must not be null!");
        this.rawEventQueue = Objects.requireNonNull(rawEventQueue, "rawEventQueue must not be null!");
        this.instanceEventQueuesMapping = Objects.requireNonNull(instanceEventQueuesMapping, "instanceEventQueuesMapping must not be null!");
    }

    @GetMapping("/threads")
    public ThreadsInfo getThreadsStatus() {
        ThreadsInfo result = new ThreadsInfo();

        result.requestTimestamp = LocalDateTime.now(ZoneId.systemDefault());

        boolean current = runtime.getInternalEventHandlerThread().isAlive();
        boolean containingDeadThreads = !current;
        result.internalEventHandlerAlive = current;

        Thread asyncEventHandlerThread = runtime.getAsyncEventHandlerThread();
        if (asyncEventHandlerThread != null) {
            current = asyncEventHandlerThread.isAlive();
            containingDeadThreads = containingDeadThreads || !current;
            result.asyncEventHandlerThread = current;
        }
        else {
            result.asyncEventHandlerThread = false;
        }

        result.instanceEventHandlers = new HashMap<>();
        Map<InstanceId, Thread> instanceEventHandlers = runtime.getInstanceEventHandlerThreads();
        for (Map.Entry<InstanceId, Thread> entry : instanceEventHandlers.entrySet()) {
            String key = entry.getKey().getId();
            Thread thread = entry.getValue();
            current = thread != null && thread.isAlive();
            containingDeadThreads = containingDeadThreads || !current;
            result.instanceEventHandlers.put(key, current);
        }

        result.receiverThreads = new HashMap<>();
        Set<Thread> receiverThreads = runtime.getReceiverThreads();
        for (Thread thread : receiverThreads) {
            String name = thread.getName();
            current = thread.isAlive();
            containingDeadThreads = containingDeadThreads || !current;
            result.receiverThreads.put(name, current);
        }

        result.containingDeadThreads = containingDeadThreads;

        return result;
    }

    @GetMapping("queues")
    public QueuesInfo getQueuesInfo() {
        QueuesInfo result = new QueuesInfo();

        result.requestTimestamp = LocalDateTime.now(ZoneId.systemDefault());

        result.rawEventQueueSize = rawEventQueue.getSize();

        result.instanceEventHandlers = new HashMap<>();
        for (Map.Entry<InstanceId, BlockingQueueInternalEventQueue> entry : instanceEventQueuesMapping.entrySet()) {
            String key = entry.getKey().getId();
            BlockingQueueInternalEventQueue queue = entry.getValue();
            result.instanceEventHandlers.put(key, queue == null ? null : queue.getSize());
        }

        return result;
    }

    @GetMapping("/instance/active")
    public List<ActiveInstanceInfo> getActiveInstanceInfos() {
        List<ActiveInstanceInfo> result = new ArrayList<>();

        for (InstanceId instanceId : activeSessionIdRepository.listAllActiveInstances()) {
            LOGGER.debug("Searching for sessions of {}", instanceId);
            //TODO map dynamic to static instance
            SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(instanceId);
            try {
                activeSessionIdRepository.retrieveActiveSessionId(instanceId)
                    .flatMap(sessionRepository::retrieveSession).ifPresent(session -> {
                        ActiveInstanceInfo info = new ActiveInstanceInfo(); // NOPMD
                        info.setId(instanceId.getId());
                        info.setActiveSessionId(instanceId.getId());
                        info.setStart(session.getCreationTimestamp());

                        session.getStateValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME)
                            .ifPresent(info::setDynamicId);

                        result.add(info);
                    });
            } catch (Exception ex) {
                LOGGER.warn("Error occurred", ex);
            }
        }

        return result;
    }

    @GetMapping("/instance/{instanceId}")
    public Map<LocalDateTime, String> getSessionList(@PathVariable String instanceId) {
        Objects.requireNonNull(instanceId, "instanceId must not be null!");

        LOGGER.info("Processing /sessions request for instance {}", instanceId);

        Optional<InstanceId> oId = instanceRepository.findInstanceId(instanceId);
        if (!oId.isPresent()) {
            throw new IllegalArgumentException("No instance found for id '" + instanceId + "'!");
        }

        InstanceId id = oId.get();
        SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(id);
        Set<SessionId> sessionIds = sessionRepository.retrieveSessionIds();

        Map<LocalDateTime, String> result = new HashMap<>();

        for (SessionId sessionId : sessionIds) {
            Session session = sessionRepository.retrieveSession(sessionId).orElse(null);
            if (session == null) {
                continue;
            }
            LocalDateTime creationTimestamp = session.getCreationTimestamp();
            result.put(creationTimestamp, sessionId.getId());
        }

        return result;
    }

    @GetMapping("/instance/{instanceId}/{sessionId}")
    public SessionProcessingState getInfo(@PathVariable String instanceId, @PathVariable String sessionId) {
        Objects.requireNonNull(instanceId, "instanceId must not be null!");
        Objects.requireNonNull(sessionId, "sessionId must not be null!");

        LOGGER.info("Processing /session-info request for instance {} and session {}", instanceId, sessionId);

        Optional<InstanceId> oId = instanceRepository.findInstanceId(instanceId);
        if (!oId.isPresent()) {
            throw new IllegalArgumentException("No instance found for id '" + instanceId + "'!");
        }

        InstanceId id = oId.get();
        SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(id);
        Set<SessionId> allSessionIds = sessionRepository.retrieveSessionIds();
        SessionId sid = null;
        for (SessionId current : allSessionIds) {
            if (sessionId.equals(current.getId())) {
                sid = current;
                break;
            }
        }
        if (sid == null) {
            throw new IllegalStateException("No sessionId '" + sessionId + "' found!");
        }

        Session session = sessionRepository.retrieveSession(sid).orElse(null);
        if (session == null) {
            throw new IllegalStateException("Missing expected session for '" + sessionId + "'!");
        }

        return session.getProcessingState();
    }

    static class ActiveInstanceInfo {
        private String id;
        private String dynamicId;
        private String activeSessionId;
        private LocalDateTime start;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDynamicId() {
            return dynamicId;
        }

        public void setDynamicId(String dynamicId) {
            this.dynamicId = dynamicId;
        }

        public String getActiveSessionId() {
            return activeSessionId;
        }

        public void setActiveSessionId(String activeSessionId) {
            this.activeSessionId = activeSessionId;
        }

        public LocalDateTime getStart() {
            return start;
        }

        public void setStart(LocalDateTime start) {
            this.start = start;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ActiveInstanceInfo)) return false;
            ActiveInstanceInfo that = (ActiveInstanceInfo) o;
            return Objects.equals(id, that.id)
                && Objects.equals(dynamicId, that.dynamicId)
                && Objects.equals(activeSessionId, that.activeSessionId)
                && Objects.equals(start, that.start);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, dynamicId, activeSessionId, start);
        }

        @Override
        public String toString() {
            return "ActiveInstanceInfo{" +
                "id='" + id + '\'' +
                ", dynamicId='" + dynamicId + '\'' +
                ", activeSessionId='" + activeSessionId + '\'' +
                ", start=" + start +
                '}';
        }
    }

    static class ThreadsInfo {
        private LocalDateTime requestTimestamp;
        private boolean internalEventHandlerAlive;
        private boolean asyncEventHandlerThread;
        private Map<String, Boolean> instanceEventHandlers;
        private Map<String, Boolean> receiverThreads;

        private boolean containingDeadThreads;

        public LocalDateTime getRequestTimestamp() {
            return requestTimestamp;
        }

        public void setRequestTimestamp(LocalDateTime requestTimestamp) {
            this.requestTimestamp = requestTimestamp;
        }

        public boolean isInternalEventHandlerAlive() {
            return internalEventHandlerAlive;
        }

        public void setInternalEventHandlerAlive(boolean internalEventHandlerAlive) {
            this.internalEventHandlerAlive = internalEventHandlerAlive;
        }

        public boolean isAsyncEventHandlerThread() {
            return asyncEventHandlerThread;
        }

        public void setAsyncEventHandlerThread(boolean asyncEventHandlerThread) {
            this.asyncEventHandlerThread = asyncEventHandlerThread;
        }

        public Map<String, Boolean> getInstanceEventHandlers() {
            return instanceEventHandlers;
        }

        public void setInstanceEventHandlers(Map<String, Boolean> instanceEventHandlers) {
            this.instanceEventHandlers = instanceEventHandlers;
        }

        public Map<String, Boolean> getReceiverThreads() {
            return receiverThreads;
        }

        public void setReceiverThreads(Map<String, Boolean> receiverThreads) {
            this.receiverThreads = receiverThreads;
        }

        public boolean isContainingDeadThreads() {
            return containingDeadThreads;
        }

        public void setContainingDeadThreads(boolean containingDeadThreads) {
            this.containingDeadThreads = containingDeadThreads;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ThreadsInfo)) return false;
            ThreadsInfo that = (ThreadsInfo) o;
            return Objects.equals(requestTimestamp, that.requestTimestamp)
                && internalEventHandlerAlive == that.internalEventHandlerAlive
                && asyncEventHandlerThread == that.asyncEventHandlerThread
                && Objects.equals(instanceEventHandlers, that.instanceEventHandlers)
                && Objects.equals(receiverThreads, that.receiverThreads);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                requestTimestamp,
                internalEventHandlerAlive,
                asyncEventHandlerThread,
                instanceEventHandlers,
                receiverThreads
            );
        }

        @Override
        public String toString() {
            return "ThreadsInfo{" +
                "requestTimestamp=" + requestTimestamp +
                ", containingDeadThreads=" + containingDeadThreads +
                ", internalEventHandlerAlive=" + internalEventHandlerAlive +
                ", processorEventHandlerAlive=" + asyncEventHandlerThread +
                ", instanceEventHandlers=" + instanceEventHandlers +
                ", receiverThreads=" + receiverThreads +
                '}';
        }
    }

    static class QueuesInfo {
        private LocalDateTime requestTimestamp;
        private int rawEventQueueSize;
        private Map<String, Integer> instanceEventHandlers;

        public LocalDateTime getRequestTimestamp() {
            return requestTimestamp;
        }

        public void setRequestTimestamp(LocalDateTime requestTimestamp) {
            this.requestTimestamp = requestTimestamp;
        }

        public int getRawEventQueueSize() {
            return rawEventQueueSize;
        }

        public void setRawEventQueueSize(int rawEventQueueSize) {
            this.rawEventQueueSize = rawEventQueueSize;
        }

        public Map<String, Integer> getInstanceEventHandlers() {
            return instanceEventHandlers;
        }

        public void setInstanceEventHandlers(Map<String, Integer> instanceEventHandlers) {
            this.instanceEventHandlers = instanceEventHandlers;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof QueuesInfo)) return false;
            QueuesInfo that = (QueuesInfo) o;
            return rawEventQueueSize == that.rawEventQueueSize
                && Objects.equals(requestTimestamp, that.requestTimestamp)
                && Objects.equals(instanceEventHandlers, that.instanceEventHandlers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(requestTimestamp, rawEventQueueSize, instanceEventHandlers);
        }

        @Override
        public String toString() {
            return "QueuesInfo{" +
                "requestTimestamp=" + requestTimestamp +
                ", rawEventQueueSize=" + rawEventQueueSize +
                ", instanceEventHandlers=" + instanceEventHandlers +
                '}';
        }
    }
}
