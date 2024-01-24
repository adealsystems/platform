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

package org.adealsystems.platform.orchestrator.registry;

import java.util.Objects;
import java.util.regex.Pattern;

public final class MessageEvent implements EventDescriptor {
    private final String id;
    private boolean startEvent;
    private boolean stopEvent;
    private String name;
    private Pattern pattern;

    public static MessageEvent forId(String id){
        return new MessageEvent(id);
    }

    private MessageEvent(String id) {
        this.id = Objects.requireNonNull(id, "id must not be null!");
    }

    public MessageEvent asStartEvent() {
        this.startEvent = true;
        return this;
    }

    public MessageEvent asStopEvent() {
        this.stopEvent = true;
        return this;
    }

    @Override
    public boolean isValid() {
        boolean nameEmpty = name == null || name.isEmpty();
        boolean patternEmpty = pattern == null;
        return !(nameEmpty && patternEmpty);
    }

    public MessageEvent withName(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null!");
        return this;
    }

    public MessageEvent withPattern(String pattern) {
        this.pattern = Pattern.compile(Objects.requireNonNull(pattern, "pattern must not be null!"));
        return this;
    }

    public MessageEvent withPattern(Pattern pattern) {
        this.pattern = Objects.requireNonNull(pattern, "pattern must not be null!");
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isStartEvent() {
        return startEvent;
    }

    @Override
    public boolean isStopEvent() {
        return stopEvent;
    }

    public String getName() {
        return name;
    }

    public Pattern getPattern() {
        return pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageEvent that = (MessageEvent) o;
        return startEvent == that.startEvent
            && stopEvent == that.stopEvent
            && Objects.equals(id, that.id)
            && Objects.equals(name, that.name)
            && Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, startEvent, stopEvent, name, pattern);
    }

    @Override
    public String toString() {
        return "MessageEvent{" +
            "id='" + id + '\'' +
            ", startEvent=" + startEvent +
            ", stopEvent=" + stopEvent +
            ", name='" + name + '\'' +
            ", pattern=" + pattern +
            '}';
    }
}
