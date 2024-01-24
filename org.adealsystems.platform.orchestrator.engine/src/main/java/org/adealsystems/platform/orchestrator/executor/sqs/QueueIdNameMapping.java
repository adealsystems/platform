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

package org.adealsystems.platform.orchestrator.executor.sqs;

import java.util.Map;

public class QueueIdNameMapping {
    private final Map<String, String> queueNameMap;

    public QueueIdNameMapping(Map<String, String> queueNameMap) {
        this.queueNameMap = queueNameMap;
    }

    public Map<String, String> getQueueNameMap() {
        return queueNameMap;
    }
}
