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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CommandExecutionCompletedMessageEvent implements EventDescriptor {
    public static final String COMMAND_ID_GROUP_NAME = "command";
    public static final String INSTANCE_ID_GROUP_NAME = "instance";

    private static final String PREFIX = "Command execution result ";

    public static final String LEGACY_COMMAND_EXECUTION_COMPLETED_MESSAGE_PREFIX
        = PREFIX + "of ";
    public static final String COMMAND_EXECUTION_COMPLETED_MESSAGE_PREFIX
        = PREFIX + "for instance ${" + INSTANCE_ID_GROUP_NAME + "} of ";
    public static final String COMMAND_EXECUTION_COMPLETED_MESSAGE
        = PREFIX + "for instance (?<" + INSTANCE_ID_GROUP_NAME + ">.*) of (?<" + COMMAND_ID_GROUP_NAME + ">.*)";
    public static final Pattern COMMAND_EXECUTION_COMPLETED_MESSAGE_PATTERN
        = Pattern.compile(COMMAND_EXECUTION_COMPLETED_MESSAGE);

    private final String id;
    private boolean stopEvent;
    private String sessionRegistryName;
    private boolean repeatable = false;

    private String instanceId;
    private String commandId;


    public static CommandExecutionCompletedMessageEvent fromEvent(
        String eventId,
        CommandExecutionCompletedMessageEvent message
    ){
        CommandExecutionCompletedMessageEvent result = new CommandExecutionCompletedMessageEvent(message.getId());

        result.stopEvent = message.isStopEvent();
        result.sessionRegistryName = message.getSessionRegistryName();
        result.repeatable = message.isRepeatable();

        Matcher matcher = COMMAND_EXECUTION_COMPLETED_MESSAGE_PATTERN.matcher(eventId);
        if (matcher.matches()) {
            result.commandId = matcher.group(COMMAND_ID_GROUP_NAME);
            result.instanceId = matcher.group(INSTANCE_ID_GROUP_NAME);
        }

        return result;
    }

    public static CommandExecutionCompletedMessageEvent forId(String id){
        return new CommandExecutionCompletedMessageEvent(id);
    }

    private CommandExecutionCompletedMessageEvent(String id) {
        this.id = Objects.requireNonNull(id, "id must not be null!");
    }

    public CommandExecutionCompletedMessageEvent asStopEvent() {
        this.stopEvent = true;
        return this;
    }

    public CommandExecutionCompletedMessageEvent forSessionRegistry(String sessionRegistryName) {
        this.sessionRegistryName = Objects.requireNonNull(sessionRegistryName, "sessionRegistryName must not be null!");
        return this;
    }

    public CommandExecutionCompletedMessageEvent asRepeatable() {
        this.repeatable = true;
        return this;
    }

    @Override
    public boolean isValid() {
        boolean nameEmpty = sessionRegistryName == null || sessionRegistryName.isEmpty();
        return !(nameEmpty);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isStartEvent() {
        // Command execution completed message event cannot be a start event of the same session!
        return false;
    }

    @Override
    public boolean isStopEvent() {
        return stopEvent;
    }

    @Override
    public boolean isRepeatable() {
        return this.repeatable;
    }

    public String getSessionRegistryName() {
        return sessionRegistryName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getCommandId() {
        return commandId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandExecutionCompletedMessageEvent that = (CommandExecutionCompletedMessageEvent) o;
        return stopEvent == that.stopEvent
            && Objects.equals(id, that.id)
            && Objects.equals(sessionRegistryName, that.sessionRegistryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, stopEvent, sessionRegistryName);
    }

    @Override
    public String toString() {
        return "CommandExecutionCompletedMessageEvent{" +
            "id='" + id + '\'' +
            ", stopEvent=" + stopEvent +
            ", sessionRegistryName='" + sessionRegistryName + '\'' +
            '}';
    }
}
