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

import java.util.Collection;
import java.util.Optional;

public interface InstanceRepository {
    Optional<InstanceId> findInstanceId(String id);
    Collection<InstanceId> retrieveInstanceIds();
    Instance createInstance(InstanceId id);
    Optional<Instance> retrieveInstance(InstanceId id);
    Instance retrieveOrCreateInstance(InstanceId id);
    void updateInstance(Instance session);
    boolean deleteInstance(InstanceId id);
}
