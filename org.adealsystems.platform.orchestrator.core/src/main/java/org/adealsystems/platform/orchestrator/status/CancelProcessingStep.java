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

package org.adealsystems.platform.orchestrator.status;


import org.adealsystems.platform.orchestrator.InternalEvent;

import java.util.Objects;

public class CancelProcessingStep extends EventProcessingStep {
    private final String name;

    public static CancelProcessingStep success(InternalEvent event, String name) {
        return new CancelProcessingStep(true, event, buildDefaultMessage(name), name);
    }

    public static CancelProcessingStep failed(InternalEvent event, String name) {
        return new CancelProcessingStep(false, event, buildDefaultMessage(name), name);
    }

    public static CancelProcessingStep failed(InternalEvent event, String name, String message) {
        return new CancelProcessingStep(false, event, message, name);
    }

    private static String buildDefaultMessage(String name) {
        return "Cancel event " + name + " arrived";
    }

    public CancelProcessingStep(boolean success, InternalEvent event, String message, String name) {
        super(success, event, message);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CancelProcessingStep)) return false;
        if (!super.equals(o)) return false;
        CancelProcessingStep that = (CancelProcessingStep) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name);
    }

    @Override
    public String toString() {
        return "CancelProcessingStep{" +
                "name='" + name + '\'' +
                "} " + super.toString();
    }
}
