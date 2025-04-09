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

import java.beans.ConstructorProperties;
import java.util.Map;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Instance {
    private final InstanceId id;
    private Map<String, String> configuration;

    @ConstructorProperties({"id"})
    public Instance(InstanceId id) {
        this.id = Objects.requireNonNull(id, "id must not be null!");
    }

    public InstanceId getId() {
        return id;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Instance)) return false;
        Instance instance = (Instance) o;
        return Objects.equals(id, instance.id) && Objects.equals(configuration, instance.configuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, configuration);
    }

    @Override
    public String toString() {
        return "Instance{" +
            "id=" + id +
            ", configuration=" + configuration +
            '}';
    }
}
