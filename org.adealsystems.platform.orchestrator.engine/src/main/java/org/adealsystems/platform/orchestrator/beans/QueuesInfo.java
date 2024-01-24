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

public class QueuesInfo {
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
