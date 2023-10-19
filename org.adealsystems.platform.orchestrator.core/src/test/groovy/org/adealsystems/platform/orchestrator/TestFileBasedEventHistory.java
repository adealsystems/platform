/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TestFileBasedEventHistory extends FileBasedEventHistory {

    private final List<InternalEvent> consumedEvents = new ArrayList<>();

    public TestFileBasedEventHistory(File baseDirectory, ObjectMapper objectMapper) {
        super(baseDirectory, new FileBasedRunRepository(new File(".")), objectMapper);
    }

    public TestFileBasedEventHistory(File baseDirectory, TimestampFactory timestampFactory, RunRepository runRepository, ObjectMapper objectMapper) {
        super(baseDirectory, timestampFactory, runRepository, objectMapper);
    }

    public List<InternalEvent> getConsumedEvents() {
        return consumedEvents;
    }

    @Override
    public void add(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");
        addAll(Collections.singleton(event));
    }

    @Override
    public void addAll(Iterable<InternalEvent> events) {
        for (InternalEvent event : events) {
            consumedEvents.add(event);
        }

        super.addAll(events);
    }
}
