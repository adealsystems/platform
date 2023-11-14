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

package org.adealsystems.platform.orchestrator.beans;

import java.time.LocalDateTime;
import java.util.Objects;

public class ActiveInstanceInfo {
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
