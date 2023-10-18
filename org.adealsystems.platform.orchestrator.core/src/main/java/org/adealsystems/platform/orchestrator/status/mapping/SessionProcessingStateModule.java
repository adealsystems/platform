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

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.adealsystems.platform.orchestrator.status.CancelProcessingStep;
import org.adealsystems.platform.orchestrator.status.EventProcessingStep;
import org.adealsystems.platform.orchestrator.status.FileProcessingStep;
import org.adealsystems.platform.orchestrator.status.ProcessingStep;
import org.adealsystems.platform.orchestrator.status.SessionProcessingState;
import org.adealsystems.platform.orchestrator.status.TimerProcessingStep;

public class SessionProcessingStateModule extends SimpleModule {
    private static final long serialVersionUID = 6180608143970327030L;

    public SessionProcessingStateModule() {
        super("SessionProcessingState", Version.unknownVersion());

        setMixInAnnotation(ProcessingStep.class, ProcessingStepMixin.class);
        addDeserializer(SessionProcessingState.class, new SessionProcessingStateDeserializer());
        addDeserializer(EventProcessingStep.class, new EventProcessingStepDeserializer());
        addDeserializer(FileProcessingStep.class, new FileProcessingStepDeserializer());
        addDeserializer(CancelProcessingStep.class, new CancelProcessingStepDeserializer());
        addDeserializer(TimerProcessingStep.class, new TimerProcessingStepDeserializer());
    }
}
