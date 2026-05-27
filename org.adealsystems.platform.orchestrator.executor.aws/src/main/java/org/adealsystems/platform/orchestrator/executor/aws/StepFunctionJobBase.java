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

package org.adealsystems.platform.orchestrator.executor.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class StepFunctionJobBase {
    public static final String DEFAULT_COMMAND_ID_PARAM_NAME = "commandId";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String stateMachineArn;

    private String commandIdParamName = DEFAULT_COMMAND_ID_PARAM_NAME;


    public StepFunctionJobBase(String stateMachineArn) {
        this.stateMachineArn = Objects.requireNonNull(stateMachineArn, "stateMachineArn must not be null!");
    }

    public void setCommandIdParamName(String commandIdParamName) {
        this.commandIdParamName = Objects.requireNonNull(commandIdParamName, "commandIdParamName must not be null!");
    }

    public String getStateMachineArn() {
        return stateMachineArn;
    }

    protected String createInput(String commandId, Map<String, ?> inputParameters) {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (inputParameters != null && !inputParameters.isEmpty()) {
            payload.putAll(inputParameters);
        }

        if (commandId != null) {
            payload.put(commandIdParamName, commandId);
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Unable to serialize Step Functions input payload!", ex);
        }
    }
}
