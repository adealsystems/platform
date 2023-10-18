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

package org.adealsystems.platform.orchestrator.status.mapping;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.adealsystems.platform.orchestrator.status.CancelProcessingStep;
import org.adealsystems.platform.orchestrator.status.EventProcessingStep;
import org.adealsystems.platform.orchestrator.status.FileProcessingStep;
import org.adealsystems.platform.orchestrator.status.SessionProcessingStep;
import org.adealsystems.platform.orchestrator.status.TimerProcessingStep;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = EventProcessingStep.class, name = "event"),
    @JsonSubTypes.Type(value = FileProcessingStep.class, name = "file"),
    @JsonSubTypes.Type(value = CancelProcessingStep.class, name = "cancel"),
    @JsonSubTypes.Type(value = SessionProcessingStep.class, name = "session"),
    @JsonSubTypes.Type(value = TimerProcessingStep.class, name = "timer"),
})
@SuppressWarnings({
    "PMD.AbstractClassWithoutAnyMethod",
    "PMD.AbstractClassWithoutAbstractMethod"
})
public abstract class ProcessingStepMixin {
}
