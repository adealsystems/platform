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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.adealsystems.platform.orchestrator.session.SessionUpdateOperationModule;
import org.adealsystems.platform.orchestrator.status.mapping.SessionProcessingStateModule;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.adealsystems.platform.orchestrator.SessionEventConstants.DYNAMIC_CONTENT_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.INSTANCE_ID_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_ID_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_LIFECYCLE_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SESSION_STATE_ATTRIBUTE_NAME;
import static org.adealsystems.platform.orchestrator.SessionEventConstants.SOURCE_EVENT_ATTRIBUTE_NAME;


public final class InternalEvent implements Cloneable, Serializable {
    private static final long serialVersionUID = -1958300486266138089L;

    public static final Comparator<InternalEvent> TIMESTAMP_COMPARATOR = new TimestampComparator();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.registerModule(new SessionProcessingStateModule());
        OBJECT_MAPPER.registerModule(new SessionUpdateOperationModule());
    }

    public static final String ATTR_RUN_ID = "run-id";

    private String id;
    private InternalEventType type;
    private InstanceId instanceId;
    private SessionId sessionId;

    @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = FalseFilter.class)
    private boolean processed;

    @JsonProperty("timestamp")
    private LocalDateTime timestamp;

    private ConcurrentMap<String, String> attributes;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public InstanceId getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(InstanceId instanceId) {
        this.instanceId = instanceId;
    }

    public SessionId getSessionId() {
        return sessionId;
    }

    public void setSessionId(SessionId sessionId) {
        this.sessionId = sessionId;
    }

    public InternalEventType getType() {
        return type;
    }

    public void setType(InternalEventType type) {
        this.type = type;
    }

    public ConcurrentMap<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        if (attributes == null) {
            this.attributes = null;
        } else {
            this.attributes = new ConcurrentHashMap<>(attributes);
        }
    }

    public Optional<String> getAttributeValue(String attributeName) {
        Objects.requireNonNull(attributeName, "attributeName must not be null!");
        if (attributes == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(attributes.get(attributeName));
    }

    public String getMandatoryAttributeValue(String attributeName) {
        Optional<String> oValue = getAttributeValue(attributeName);
        if (oValue.isEmpty()) {
            throw new IllegalArgumentException("Missing mandatory attribute value '" + attributeName + "'!");
        }
        return oValue.get();
    }

    public void setAttributeValue(String key, String value) {
        Objects.requireNonNull(key, "key must not be null!");

        if (value == null) {
            if (attributes != null) {
                attributes.remove(key);
                if (attributes.isEmpty()) {
                    attributes = null;
                }
            }
            return;
        }

        if (attributes == null) {
            attributes = new ConcurrentHashMap<>();
        }
        attributes.put(key, value);
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isProcessed() {
        return processed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InternalEvent)) return false;
        InternalEvent that = (InternalEvent) o;
        return processed == that.processed
            && Objects.equals(id, that.id)
            && type == that.type
            && Objects.equals(instanceId, that.instanceId)
            && Objects.equals(sessionId, that.sessionId)
            && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, instanceId, sessionId, processed, attributes);
    }

    @Override
    public String toString() {
        return "InternalEvent{" +
            "id='" + id + '\'' +
            ", type=" + type +
            ", instanceId=" + instanceId +
            ", sessionId=" + sessionId +
            ", processed=" + processed +
            ", timestamp=" + timestamp +
            ", attributes=" + attributes +
            '}';
    }

    @Override
    public InternalEvent clone() {
        try {
            InternalEvent clone = (InternalEvent) super.clone();
            clone.timestamp = timestamp;
            //clone.instanceId = instanceId;
            //clone.sessionId = sessionId;
            if (attributes != null) {
                clone.setAttributes(new HashMap<>(attributes));
            }
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new IllegalStateException("Internal event must be cloneable!", ex);
        }
    }

    public static void setMinimizedSourceEventAttribute(InternalEvent event, InternalEvent sourceEvent) {
        Objects.requireNonNull(event, "event must not be null!");
        Objects.requireNonNull(sourceEvent, "sourceEvent must not be null!");
        InternalEvent clone = sourceEvent.clone();

        // clean up attributes (use only necessary attributes)
        Map<String, String> attributes = new HashMap<>();
        clone.getAttributeValue(ATTR_RUN_ID)
            .ifPresent(it -> attributes.put(ATTR_RUN_ID, it));
        clone.getAttributeValue(INSTANCE_ID_ATTRIBUTE_NAME)
            .ifPresent(it -> attributes.put(INSTANCE_ID_ATTRIBUTE_NAME, it));
        clone.getAttributeValue(SESSION_ID_ATTRIBUTE_NAME)
            .ifPresent(it -> attributes.put(SESSION_ID_ATTRIBUTE_NAME, it));
        clone.getAttributeValue(SESSION_LIFECYCLE_ATTRIBUTE_NAME)
            .ifPresent(it -> attributes.put(SESSION_LIFECYCLE_ATTRIBUTE_NAME, it));
        clone.setAttributes(attributes);

        setSourceEventAttribute(event, clone);
    }

    public static void setSourceEventAttribute(InternalEvent event, InternalEvent sourceEvent) {
        Objects.requireNonNull(event, "event must not be null!");
        Objects.requireNonNull(sourceEvent, "sourceEvent must not be null!");

        try {
            event.setAttributeValue(SOURCE_EVENT_ATTRIBUTE_NAME, OBJECT_MAPPER.writeValueAsString(sourceEvent));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Error serializing source event as JSON!", ex);
        }
    }

    public static Optional<InternalEvent> getSourceEventAttribute(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");
        Optional<String> oSourceEvent = event.getAttributeValue(SOURCE_EVENT_ATTRIBUTE_NAME);
        if (oSourceEvent.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(OBJECT_MAPPER.readValue(oSourceEvent.get(), InternalEvent.class));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Error deserializing source event from JSON!", ex);
        }
    }

    public static void setSessionStateAttribute(InternalEvent event, Session session) {
        Objects.requireNonNull(event, "event must not be null!");
        Objects.requireNonNull(session, "session must not be null!");
        Session sessionClone = Session.copyOf(session);
        sessionClone.setProcessingState(null);
        sessionClone.setSessionUpdates(null);

        try {
            event.setAttributeValue(SESSION_STATE_ATTRIBUTE_NAME, OBJECT_MAPPER.writeValueAsString(sessionClone));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Error serializing session as JSON!", ex);
        }
    }

    public static Optional<Session> getSessionStateAttribute(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");
        Optional<String> oSession = event.getAttributeValue(SESSION_STATE_ATTRIBUTE_NAME);
        if (oSession.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(OBJECT_MAPPER.readValue(oSession.get(), Session.class));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Error deserializing session from JSON!", ex);
        }
    }

    public static Optional<String> getDynamicContentFromSessionStateAttribute(InternalEvent event) {
        Optional<Session> oSessionState = getSessionStateAttribute(event);
        if (oSessionState.isEmpty()) {
            return Optional.empty();
        }

        Session eventSession = oSessionState.get();

        return eventSession.getStateValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME);
    }

    public static Optional<String> getDynamicContentFromSourceEvent(InternalEvent event) {
        Optional<InternalEvent> oSourceEvent = getSourceEventAttribute(event);
        if (oSourceEvent.isEmpty()) {
            return Optional.empty();
        }

        InternalEvent sourceEvent = oSourceEvent.get();
        return getDynamicContentAttribute(sourceEvent);
    }

    public static void setDynamicContentAttribute(InternalEvent event, String dynamicContent) {
        Objects.requireNonNull(event, "event must not be null!");
        Objects.requireNonNull(dynamicContent, "session must not be null!");
        event.setAttributeValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME, dynamicContent);
    }

    public static Optional<String> getDynamicContentAttribute(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");
        return event.getAttributeValue(DYNAMIC_CONTENT_ATTRIBUTE_NAME);
    }

    public static String normalizeDynamicContent(String dynamicContent) {
        if (dynamicContent == null) {
            return null;
        }

        // ([,@\_\-\.0-9a-zA-Z]+)* -> ([-_][0-9a-z]+)*
        return dynamicContent
            .toLowerCase(Locale.ROOT)
            .replaceAll(",", "-c-")
            .replaceAll("@", "-a-")
            .replaceAll("%", "-p-")
            .replaceAll("\\.", "-d-");
    }

    public static InternalEvent deriveProcessedInstance(InternalEvent event) {
        InternalEvent result = event.clone();
        result.processed = true;
        return result;
    }

    public static InternalEvent deriveUnprocessedInstance(InternalEvent event) {
        InternalEvent result = event.clone();
        result.processed = false;
        return result;
    }

    public static boolean isOwnerInstance(InternalEvent event, String instanceKey) {
        Objects.requireNonNull(event, "event must not be null!");
        Objects.requireNonNull(instanceKey, "instanceKey must not be null!");

        Optional<String> oInstanceId = event.getAttributeValue(INSTANCE_ID_ATTRIBUTE_NAME);
        if (oInstanceId.isEmpty()) {
            return false;
        }

        String id = oInstanceId.get();
        return instanceKey.equals(id);
    }

    private static class TimestampComparator implements Comparator<InternalEvent> {

        @Override
        public int compare(InternalEvent event1, InternalEvent event2) {
            if (event1 == event2) { // NOPMD CompareObjectsWithEquals
                return 0;
            }
            if (event1 == null) {
                return -1;
            }
            if (event2 == null) {
                return 1;
            }
            LocalDateTime t1 = event1.getTimestamp();
            LocalDateTime t2 = event2.getTimestamp();
            if (t1 == t2) { // NOPMD CompareObjectsWithEquals
                return 0;
            }
            if (t1 == null) {
                return -1;
            }
            if (t2 == null) {
                return 1;
            }

            return t1.compareTo(t2);
        }
    }
}


