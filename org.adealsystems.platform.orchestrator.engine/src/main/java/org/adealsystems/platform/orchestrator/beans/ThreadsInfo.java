/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator.beans;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;

public class ThreadsInfo {
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
