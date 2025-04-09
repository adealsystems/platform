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

package org.adealsystems.platform.orchestrator.executor;

import java.util.Objects;

public class ExecutorResult<T> {
    private final T result;
    private final String commandId;
    private final String message;
    private final Throwable cause;

    public ExecutorResult(String message) {
        this.result = null;
        this.commandId = null;
        this.message = message;
        this.cause = null;
    }

    public ExecutorResult(String message, Throwable cause) {
        this.result = null;
        this.commandId = null;
        this.message = message;
        this.cause = cause;
    }

    public ExecutorResult(T result, String commandId) {
        this.result = result;
        this.commandId = commandId;
        this.message = null;
        this.cause = null;
    }

    public ExecutorResult(T result, String commandId, String message) {
        this.result = result;
        this.commandId = commandId;
        this.message = message;
        this.cause = null;
    }

    public ExecutorResult(T result, String commandId, String message, Throwable cause) {
        this.result = result;
        this.commandId = commandId;
        this.message = message;
        this.cause = cause;
    }

    public T getResult() {
        return result;
    }

    public String getCommandId() {
        return commandId;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutorResult)) return false;
        ExecutorResult<?> that = (ExecutorResult<?>) o;
        return Objects.equals(result, that.result) && Objects.equals(commandId, that.commandId) && Objects.equals(message, that.message) && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(result, commandId, message, cause);
    }

    @Override
    public String toString() {
        return "ExecutorResult{" +
            "result=" + result +
            ", commandId='" + commandId + '\'' +
            ", message='" + message + '\'' +
            ", cause=" + cause +
            '}';
    }
}
