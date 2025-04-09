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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;

public class RunRepositoryStub implements RunRepository {
    private final String activeRun;
    private final String waitingRun;

    public RunRepositoryStub(String activeRun, String waitingRun) {
        this.activeRun = activeRun;
        this.waitingRun = waitingRun;
    }

    @Override
    public void createRun(String id) {
        //
    }

    @Override
    public Optional<String> retrieveActiveRun() {
        return Optional.of(activeRun);
    }

    @Override
    public Optional<String> retrieveWaitingRun() {
        return Optional.of(waitingRun);
    }

    @Override
    public Optional<LocalDateTime> retrieveActiveRunStartTimestamp() {
        return Optional.of(LocalDateTime.of(
                LocalDate.parse(activeRun, DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)),
                LocalTime.MIDNIGHT)
            .plusSeconds(1)
        );
    }

    @Override
    public Optional<LocalDateTime> retrieveWaitingRunStartTimestamp() {
        return Optional.of(LocalDateTime.of(
                LocalDate.parse(waitingRun, DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)),
                LocalTime.MIDNIGHT)
            .plusSeconds(1)
        );
    }

    @Override
    public void completeRun() {
        //
    }
}
