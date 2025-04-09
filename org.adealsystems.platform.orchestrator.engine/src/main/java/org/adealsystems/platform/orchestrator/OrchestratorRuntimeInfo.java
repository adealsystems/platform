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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OrchestratorRuntimeInfo {
    private final String base;
    private final boolean starting;
    private final boolean running;
    private final boolean stopping;

    private final String rawEventSenderClassName;
    private final boolean rawEventSenderEmpty;

    private String asyncJobReceiverRunnableClassName;
    private final boolean asyncEventHandlerThreadAlive;

    private final Map<String, String> receiverRunnableClassNames;
    private final Map<String, Boolean> receiverRunnableThreadsAlive;

    private final boolean internalEventReceiverThreadAlive;
    private final int internalEventReceiverQueueSize;

    private final Map<String, Boolean> instanceEventHandlerThreadAlive;
    private final Map<String, Integer> instanceEventHandlerQueueSize;
    private final Map<String, Integer> instanceEventHandlerOrphanSize;

    public OrchestratorRuntimeInfo(OrchestratorRuntime runtime) {
        this.base = runtime.getBasePath();

        this.starting = runtime.getStarting().get();
        this.running = runtime.getRunning().get();
        this.stopping = runtime.getStopping().get();

        InternalEventSender rawEventSender = runtime.getRawEventSender();
        this.rawEventSenderClassName = rawEventSender.getClass().getName();
        this.rawEventSenderEmpty = rawEventSender.isEmpty();

        JobReceiverRunnable asyncJobReceiverRunnable = runtime.getAsyncJobReceiverRunnable();
        if (asyncJobReceiverRunnable != null) {
            this.asyncJobReceiverRunnableClassName = asyncJobReceiverRunnable.getClass().getName();
            this.asyncEventHandlerThreadAlive = runtime.getAsyncEventHandlerThread().isAlive();
        } else {
            this.asyncEventHandlerThreadAlive = false;
        }

        this.receiverRunnableClassNames = new HashMap<>();
        this.receiverRunnableThreadsAlive = new HashMap<>();
        Map<String, Runnable> receiverRunnable = runtime.getReceiverRunnable();
        Map<String, Thread> receiverThreads = runtime.getReceiverThreads();
        for (Map.Entry<String, Runnable> entry : receiverRunnable.entrySet()) {
            String source = entry.getKey();
            Runnable receiver = entry.getValue();
            this.receiverRunnableClassNames.put(source, receiver.getClass().getName());
            this.receiverRunnableThreadsAlive.put(source, receiverThreads.get(source).isAlive());
        }

        this.internalEventReceiverThreadAlive = runtime.getInternalEventHandlerThread().isAlive();
        InternalEventHandlerRunnable internalEventHandlerRunnable = runtime.getInternalEventHandlerRunnable();
        InternalEventReceiver rawEventReceiver = internalEventHandlerRunnable.getRawEventReceiver();
        this.internalEventReceiverQueueSize = rawEventReceiver.getSize();

        OrphanEventSource eventHistory = (OrphanEventSource) runtime.getEventHistory();

        this.instanceEventHandlerThreadAlive = new HashMap<>();
        this.instanceEventHandlerQueueSize = new HashMap<>();
        this.instanceEventHandlerOrphanSize = new HashMap<>();
        Map<InstanceId, InstanceEventHandlerRunnable> instanceRunnable = runtime.getInstanceEventHandlerRunnable();
        Map<InstanceId, Thread> instanceThreads = runtime.getInstanceEventHandlerThreads();
        for (Map.Entry<InstanceId, InstanceEventHandlerRunnable> entry : instanceRunnable.entrySet()) {
            InstanceId instanceId = entry.getKey();
            String instanceRef = instanceId.getId();
            InstanceEventHandlerRunnable currentRunnable = entry.getValue();
            Thread currentThread = instanceThreads.get(instanceId);
            this.instanceEventHandlerThreadAlive.put(instanceRef, currentThread.isAlive());
            InternalEventReceiver eventReceiver = currentRunnable.getEventReceiver();
            this.instanceEventHandlerQueueSize.put(instanceRef, eventReceiver.getSize());
            this.instanceEventHandlerOrphanSize.put(instanceRef, eventHistory.getOrphansCount(instanceId));
        }
    }

    public String getBase() {
        return base;
    }

    public boolean isStarting() {
        return starting;
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isStopping() {
        return stopping;
    }

    public String getRawEventSenderClassName() {
        return rawEventSenderClassName;
    }

    public boolean isRawEventSenderEmpty() {
        return rawEventSenderEmpty;
    }

    public String getAsyncJobReceiverRunnableClassName() {
        return asyncJobReceiverRunnableClassName;
    }

    public boolean isAsyncEventHandlerThreadAlive() {
        return asyncEventHandlerThreadAlive;
    }

    public Map<String, String> getReceiverRunnableClassNames() {
        return receiverRunnableClassNames;
    }

    public Map<String, Boolean> getReceiverRunnableThreadsAlive() {
        return receiverRunnableThreadsAlive;
    }

    public boolean isInternalEventReceiverThreadAlive() {
        return internalEventReceiverThreadAlive;
    }

    public int getInternalEventReceiverQueueSize() {
        return internalEventReceiverQueueSize;
    }

    public Map<String, Boolean> getInstanceEventHandlerThreadAlive() {
        return instanceEventHandlerThreadAlive;
    }

    public Map<String, Integer> getInstanceEventHandlerQueueSize() {
        return instanceEventHandlerQueueSize;
    }

    public Map<String, Integer> getInstanceEventHandlerOrphanSize() {
        return instanceEventHandlerOrphanSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrchestratorRuntimeInfo that = (OrchestratorRuntimeInfo) o;
        return starting == that.starting
            && running == that.running
            && stopping == that.stopping
            && rawEventSenderEmpty == that.rawEventSenderEmpty
            && asyncEventHandlerThreadAlive == that.asyncEventHandlerThreadAlive
            && internalEventReceiverThreadAlive == that.internalEventReceiverThreadAlive
            && internalEventReceiverQueueSize == that.internalEventReceiverQueueSize
            && Objects.equals(base, that.base)
            && Objects.equals(rawEventSenderClassName, that.rawEventSenderClassName)
            && Objects.equals(asyncJobReceiverRunnableClassName, that.asyncJobReceiverRunnableClassName)
            && Objects.equals(receiverRunnableClassNames, that.receiverRunnableClassNames)
            && Objects.equals(receiverRunnableThreadsAlive, that.receiverRunnableThreadsAlive)
            && Objects.equals(instanceEventHandlerThreadAlive, that.instanceEventHandlerThreadAlive)
            && Objects.equals(instanceEventHandlerQueueSize, that.instanceEventHandlerQueueSize)
            && Objects.equals(instanceEventHandlerOrphanSize, that.instanceEventHandlerOrphanSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            base,
            starting,
            running,
            stopping,
            rawEventSenderClassName,
            rawEventSenderEmpty,
            asyncJobReceiverRunnableClassName,
            asyncEventHandlerThreadAlive,
            receiverRunnableClassNames,
            receiverRunnableThreadsAlive,
            internalEventReceiverThreadAlive,
            internalEventReceiverQueueSize,
            instanceEventHandlerThreadAlive,
            instanceEventHandlerQueueSize,
            instanceEventHandlerOrphanSize
        );
    }

    @Override
    public String toString() {
        return "OrchestratorRuntimeInfo{" +
            "base='" + base + '\'' +
            ", starting=" + starting +
            ", running=" + running +
            ", stopping=" + stopping +
            ", rawEventSenderClassName='" + rawEventSenderClassName + '\'' +
            ", rawEventSenderEmpty=" + rawEventSenderEmpty +
            ", asyncJobReceiverRunnableClassName='" + asyncJobReceiverRunnableClassName + '\'' +
            ", asyncEventHandlerThreadAlive=" + asyncEventHandlerThreadAlive +
            ", receiverRunnableClassNames=" + receiverRunnableClassNames +
            ", receiverRunnableThreadsAlive=" + receiverRunnableThreadsAlive +
            ", internalEventReceiverThreadAlive=" + internalEventReceiverThreadAlive +
            ", internalEventReceiverQueueSize=" + internalEventReceiverQueueSize +
            ", instanceEventHandlerThreadAlive=" + instanceEventHandlerThreadAlive +
            ", instanceEventHandlerQueueSize=" + instanceEventHandlerQueueSize +
            ", instanceEventHandlerOrphanSize" + instanceEventHandlerOrphanSize +
            '}';
    }
}
