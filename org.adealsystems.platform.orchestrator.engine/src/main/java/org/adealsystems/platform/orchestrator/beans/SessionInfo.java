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

import java.util.Objects;

public class SessionInfo {
    private String instanceId;
    private String sessionId;
    private String startTimestamp;
    private String lastUpdatedTimestamp;
    private String endTimestamp;
    private String state;
    private String message;

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(String startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public String getLastUpdatedTimestamp() {
        return lastUpdatedTimestamp;
    }

    public void setLastUpdatedTimestamp(String lastUpdatedTimestamp) {
        this.lastUpdatedTimestamp = lastUpdatedTimestamp;
    }

    public String getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(String endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionInfo that = (SessionInfo) o;
        return Objects.equals(instanceId, that.instanceId)
            && Objects.equals(sessionId, that.sessionId)
            && Objects.equals(startTimestamp, that.startTimestamp)
            && Objects.equals(lastUpdatedTimestamp, that.lastUpdatedTimestamp)
            && Objects.equals(endTimestamp, that.endTimestamp)
            && Objects.equals(state, that.state)
            && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceId, sessionId, startTimestamp, lastUpdatedTimestamp, endTimestamp, state, message);
    }

    @Override
    public String toString() {
        return "SessionInfo{" +
            "instanceId='" + instanceId + '\'' +
            ", sessionId='" + sessionId + '\'' +
            ", startTimestamp=" + startTimestamp +
            ", lastUpdatedTimestamp=" + lastUpdatedTimestamp +
            ", endTimestamp=" + endTimestamp +
            ", state=" + state +
            ", message='" + message + '\'' +
            '}';
    }
}
