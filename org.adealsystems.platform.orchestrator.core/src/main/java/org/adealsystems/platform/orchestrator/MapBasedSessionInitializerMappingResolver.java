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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MapBasedSessionInitializerMappingResolver implements SessionInitializerMappingResolver {
    private final Map<InstanceId, SessionInitializer> initializers;

    public MapBasedSessionInitializerMappingResolver(Map<InstanceId, SessionInitializer> initializers) {
        Objects.requireNonNull(initializers, "initializers must not be null!");
        this.initializers = Collections.unmodifiableMap(new HashMap<>(initializers));
    }

    @Override
    public Map<InstanceId, SessionInitializer> resolveMapping() {
        return initializers;
    }
}
