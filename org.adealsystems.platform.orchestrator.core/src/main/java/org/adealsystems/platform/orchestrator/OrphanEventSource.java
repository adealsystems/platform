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

import org.adealsystems.platform.io.Drain;

public interface OrphanEventSource {

    /**
     * Drains the orphan events for the instance defined in idTuple into the given drain, if orphan events exist.
     *
     * @param eventClassifier EventClassifier which will handle event.
     * @param idTuple IdTuple with both instanceId and sessionId defined.
     * @param eventDrain Drain used to drain the orphan events into, if any. The drain is not closed by this method.
     * @return true if events were drained, false otherwise.
     * @throws NullPointerException if idTuple or eventDrain are null.
     * @throws IllegalArgumentException if idTuple is missing either instanceId or sessionId
     */
    boolean drainOrphanEventsInto(InternalEventClassifier eventClassifier, EventAffiliation idTuple, Drain<InternalEvent> eventDrain);

    int getOrphansCount(InstanceId instanceId);
}
