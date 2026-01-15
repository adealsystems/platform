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

package org.adealsystems.platform.orchestrator.beans;

import org.adealsystems.platform.orchestrator.InternalEvent;

import java.time.LocalDateTime;
import java.util.Objects;

public class QueueHeadInfo {
    private LocalDateTime requestTimestamp;
    private boolean present;
    private InternalEvent headEvent;

    public LocalDateTime getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(LocalDateTime requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

    public boolean isPresent() {
        return present;
    }

    public void setPresent(boolean present) {
        this.present = present;
    }

    public InternalEvent getHeadEvent() {
        return headEvent;
    }

    public void setHeadEvent(InternalEvent headEvent) {
        this.headEvent = headEvent;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        QueueHeadInfo that = (QueueHeadInfo) o;
        return present == that.present
            && Objects.equals(requestTimestamp, that.requestTimestamp)
            && Objects.equals(headEvent, that.headEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestTimestamp, present, headEvent);
    }

    @Override
    public String toString() {
        return "QueueHeadInfo{" +
            "requestTimestamp=" + requestTimestamp +
            ", present=" + present +
            ", headEvent=" + headEvent +
            '}';
    }
}
