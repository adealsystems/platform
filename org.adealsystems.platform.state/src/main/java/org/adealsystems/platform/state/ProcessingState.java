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

package org.adealsystems.platform.state;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ProcessingState {
    private List<String> errors;
    private Map<String, String> attributes;

    private static final ProcessingState SUCCESS = new ProcessingState();

    public static ProcessingState createSuccessState() {
        return SUCCESS;
    }

    public static ProcessingState createFailedState(String... errors) {
        Objects.requireNonNull(errors, "errors must not be null!");
        return createFailedState(Arrays.asList(errors));
    }

    public static ProcessingState createFailedState(List<String> errors) {
        Objects.requireNonNull(errors, "errors must not be null!");
        if (errors.isEmpty()) {
            throw new IllegalArgumentException("errors must not be empty!");
        }

        ProcessingState state = new ProcessingState();
        state.setErrors(errors);
        return state;
    }

    public static ProcessingState createFailedState(String message, Throwable throwable) {
        Objects.requireNonNull(message, "message must not be null!");
        Objects.requireNonNull(throwable, "throwable must not be null!");
        return createFailedStateInternal(message, throwable);
    }

    public static ProcessingState createFailedState(Throwable throwable) {
        Objects.requireNonNull(throwable, "throwable must not be null!");
        return createFailedStateInternal(null, throwable);
    }

    private static ProcessingState createFailedStateInternal(String message, Throwable throwable) {
        List<String> errors = new ArrayList<>();

        if (message != null) {
            errors.add(message);
        }

        Throwable current = throwable;
        do {
            errors.add(current.toString());
            current = current.getCause();
        } while (current != null);

        ProcessingState state = new ProcessingState();
        state.setErrors(errors);
        return state;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }

    public boolean hasErrors() {
        return errors != null && !errors.isEmpty();
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcessingState that = (ProcessingState) o;
        return Objects.equals(errors, that.errors) && Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errors, attributes);
    }

    @Override
    public String toString() {
        return "ProcessingState{" +
            "errors=" + errors +
            ", attributes=" + attributes +
            '}';
    }
}
