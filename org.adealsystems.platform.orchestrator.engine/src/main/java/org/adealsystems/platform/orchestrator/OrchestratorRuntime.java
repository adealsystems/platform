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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class OrchestratorRuntime {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrchestratorRuntime.class);

    private final String orchestratorBasePath;

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean starting = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    private final InstanceEventHandlerResolver instanceEventHandlerResolver;
    private final InstanceEventReceiverResolver instanceEventReceiverResolver;
    private final SessionRepositoryFactory sessionRepositoryFactory;
    private final InternalEventSender rawEventSender;
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    private final ActiveSessionIdRepository activeSessionIdRepository;
    private final Map<String, Runnable> receiverRunnables;
    private final InternalEventHandlerRunnable internalEventHandlerRunnable;
    private final InstanceRepository instanceRepository;
    private final TimestampFactory timestampFactory;
    private final EventHistory eventHistory;

    private final JobReceiverRunnable asyncJobReceiverRunnable;
    private Thread asyncEventHandlerThread;

    private final Set<Thread> receiverThreads = new HashSet<>();
    private Thread internalEventHandlerThread;
    private final Map<InstanceId, Thread> instanceEventHandlerThreads = new HashMap<>();

    @SuppressWarnings("PMD.ExcessiveParameterList")
    public OrchestratorRuntime(
        String orchestratorBasePath,
        InstanceEventHandlerResolver instanceEventHandlerResolver,
        InstanceEventReceiverResolver instanceEventReceiverResolver,
        SessionRepositoryFactory sessionRepositoryFactory,
        InternalEventSender rawEventSender,
        Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
        ActiveSessionIdRepository activeSessionIdRepository,
        Map<String, Runnable> receiverRunnables,
        InternalEventHandlerRunnable internalEventHandlerRunnable,
        JobReceiverRunnable asyncJobReceiverRunnable,
        InstanceRepository instanceRepository,
        TimestampFactory timestampFactory,
        EventHistory eventHistory
    ) {
        this.orchestratorBasePath = Objects.requireNonNull(orchestratorBasePath, "orchestratorBasePath must not be null!");
        this.instanceEventHandlerResolver = Objects.requireNonNull(instanceEventHandlerResolver, "instanceEventHandlerResolver must not be null!");
        this.instanceEventReceiverResolver = Objects.requireNonNull(instanceEventReceiverResolver, "instanceEventReceiverResolver must not be null!");
        this.sessionRepositoryFactory = Objects.requireNonNull(sessionRepositoryFactory, "sessionRepositoryFactory must not be null!");
        this.rawEventSender = Objects.requireNonNull(rawEventSender, "rawEventSender must not be null!");
        this.uncaughtExceptionHandler = Objects.requireNonNull(uncaughtExceptionHandler, "uncaughtExceptionHandler must not be null!");
        this.activeSessionIdRepository = Objects.requireNonNull(activeSessionIdRepository, "activeSessionIdRepository must not be null!");
        this.receiverRunnables = Objects.requireNonNull(receiverRunnables, "receiverRunnables must not be null!");
        this.internalEventHandlerRunnable = Objects.requireNonNull(internalEventHandlerRunnable, "internalEventHandlerRunnable must not be null!");
        this.instanceRepository = Objects.requireNonNull(instanceRepository, "instanceRepository must not be null!");
        this.timestampFactory = Objects.requireNonNull(timestampFactory, "timestampFactory must not be null!");
        this.eventHistory = Objects.requireNonNull(eventHistory, "eventHistory must not be null!");
        this.asyncJobReceiverRunnable = asyncJobReceiverRunnable;
    }

    public Thread getAsyncEventHandlerThread() {
        return asyncEventHandlerThread;
    }

    public Set<Thread> getReceiverThreads() {
        return receiverThreads;
    }

    public Thread getInternalEventHandlerThread() {
        return internalEventHandlerThread;
    }

    public Map<InstanceId, Thread> getInstanceEventHandlerThreads() {
        return instanceEventHandlerThreads;
    }

    public void start() {
        File runningFile = resolveRunningFile();
        if (runningFile.exists()) {
            LOGGER.warn("There is already another orchestrator running!");
            System.exit(1); //NOPMD
        }
        try {
            Files.createFile(runningFile.toPath());
        } catch (IOException e) {
            LOGGER.error("Create running file failed!", e);
            System.exit(1); //NOPMD
        }

        lock.lock();
        try {
            if (running.get()) {
                LOGGER.warn("Runtime is already running!");
                return; // TODO: exception?
            }

            starting.set(true);

            Collection<InstanceId> instanceIds = instanceRepository.retrieveInstanceIds();
            for (InstanceId instanceId : instanceIds) {
                Optional<InstanceEventHandler> oEventHandler = instanceEventHandlerResolver.resolveHandler(instanceId);
                if (!oEventHandler.isPresent()) {
                    LOGGER.warn("No instance event handler found for instance '{}'!", instanceId);
                    continue;
                }

                InternalEventReceiver eventReceiver = instanceEventReceiverResolver.resolveEventReceiver(instanceId);
                if (eventReceiver == null) {
                    LOGGER.warn("No event receiver found for instance '{}'!", instanceId);
                    continue;
                }

                InstanceEventHandler eventHandler = oEventHandler.get();
                createInstanceThread(instanceId, rawEventSender, eventReceiver, eventHandler, timestampFactory, eventHistory);
            }

            internalEventHandlerThread = new Thread(internalEventHandlerRunnable, "raw-event-handler");
            internalEventHandlerThread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
            LOGGER.info("Starting {} thread", internalEventHandlerThread.getName());
            internalEventHandlerThread.start();

            for (Map.Entry<String, Runnable> entry : receiverRunnables.entrySet()) {
                Thread thread = new Thread(entry.getValue(), "event-receiver-" + entry.getKey()); // NOPMD AvoidInstantiatingObjectsInLoops
                thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
                thread.setDaemon(true);
                receiverThreads.add(thread);
                LOGGER.info("Starting {} thread", thread.getName());
                thread.start();
            }

            if (asyncJobReceiverRunnable != null) {
                asyncEventHandlerThread = new Thread(asyncJobReceiverRunnable, "async-jobs-handler");
                asyncEventHandlerThread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
                LOGGER.info("Starting {} thread", asyncEventHandlerThread.getName());
                asyncEventHandlerThread.start();
            }
            else {
                LOGGER.info("No async job receiver runnable configured. No corresponding thread will be started.");
            }

            starting.set(false);
            running.set(true);
        } finally {
            lock.unlock();
        }
    }

    public boolean continueRunning() {
        return resolveRunningFile().exists();
    }

    public OrchestratorStatus getStatus() {
        if (!running.get()) {
            return OrchestratorStatus.NOT_RUNNING;
        }
        if (starting.get()) {
            return OrchestratorStatus.STARTING;
        }
        if (stopping.get()) {
            return OrchestratorStatus.STOPPING;
        }

        boolean internalQueueEmpty = rawEventSender.isEmpty();
        if (!internalQueueEmpty) {
            return OrchestratorStatus.RUNNING;
        }

        Collection<InstanceId> allIds = instanceRepository.retrieveInstanceIds();
        for (InstanceId instanceId : allIds) {
            Optional<SessionId> oSession = activeSessionIdRepository.retrieveActiveSessionId(instanceId);
            if (oSession.isPresent()) {
                return OrchestratorStatus.RUNNING;
            }
        }

        return OrchestratorStatus.IDLE;
    }

    public void stop() {
        lock.lock();
        try {
            if (!running.get()) {
                LOGGER.warn("Runtime is not running!");
                return; // TODO: exception?
            }

            stopping.set(true);

            if (asyncJobReceiverRunnable != null) {
                LOGGER.info("Interrupting {} thread", asyncEventHandlerThread.getName());
                asyncEventHandlerThread.interrupt();
            }

            for (Thread receiverThread : receiverThreads) {
                LOGGER.info("Interrupting {} thread", receiverThread.getName());
                receiverThread.interrupt();
            }

            LOGGER.info("Interrupting {} thread", internalEventHandlerThread.getName());
            internalEventHandlerThread.interrupt();

            for (Map.Entry<InstanceId, Thread> entry : instanceEventHandlerThreads.entrySet()) {
                Thread thread = entry.getValue();
                LOGGER.info("Interrupting {} thread", thread.getName());
                thread.interrupt();
            }

            stopping.set(false);
            running.set(false);
        } finally {
            lock.unlock();
        }
    }

    private File resolveRunningFile() {
        return new File(orchestratorBasePath + "/running.txt");
    }

    private void createInstanceThread(
        InstanceId instanceId,
        InternalEventSender rawEventSender,
        InternalEventReceiver eventReceiver,
        InstanceEventHandler eventHandler,
        TimestampFactory timestampFactory,
        EventHistory eventHistory
    ) {
        SessionRepository sessionRepository = sessionRepositoryFactory.retrieveSessionRepository(instanceId);
        InstanceEventHandlerRunnable runnable = new InstanceEventHandlerRunnable(
            instanceId,
            rawEventSender,
            eventHandler,
            sessionRepository,
            eventReceiver,
            timestampFactory,
            eventHistory
        );

        Thread thread = new Thread(runnable, "event-handler-" + instanceId.getId());
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        instanceEventHandlerThreads.put(instanceId, thread);
        LOGGER.info("Starting {} thread", thread.getName());
        thread.start();
    }

    public EventHistory getEventHistory() {
        return eventHistory;
    }

    public enum OrchestratorStatus {
        IDLE,
        STARTING,
        RUNNING,
        STOPPING,
        NOT_RUNNING
    }
}
