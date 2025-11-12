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

package org.adealsystems.platform.orchestrator.session;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = SessionAddStepOperation.class, name = "add-step"),
    @JsonSubTypes.Type(value = SessionSetProgressMaxValueOperation.class, name = "set-progress-max-value"),
    @JsonSubTypes.Type(value = SessionUpdateFailedProgressOperation.class, name = "update-failed-progress"),
    @JsonSubTypes.Type(value = SessionUpdateMessageOperation.class, name = "update-message"),
    @JsonSubTypes.Type(value = SessionUpdateProcessingStateOperation.class, name = "update-processing-state"),
    @JsonSubTypes.Type(value = SessionUpdateProgressOperation.class, name = "update-progress"),
    @JsonSubTypes.Type(value = SessionUpdateStateValueOperation.class, name = "update-state-value"),
    @JsonSubTypes.Type(value = SessionUpdateTimestampOperation.class, name = "update-timestamp"),
    @JsonSubTypes.Type(value = SessionUpdateStateOperation.class, name = "update-state"),
})
@SuppressWarnings({
    "PMD.AbstractClassWithoutAnyMethod",
    "PMD.AbstractClassWithoutAbstractMethod"
})
public abstract class SessionUpdateOperationMixin {
}
