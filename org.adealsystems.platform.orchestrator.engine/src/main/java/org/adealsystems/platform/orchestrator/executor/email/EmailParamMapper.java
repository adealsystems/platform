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

package org.adealsystems.platform.orchestrator.executor.email;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.adealsystems.platform.orchestrator.InternalEvent;
import org.adealsystems.platform.orchestrator.Session;

import java.io.IOException;

public final class EmailParamMapper {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.registerModule(new JavaTimeModule());
        MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    private EmailParamMapper() {
    }

    public static String map(InternalEvent event, Object stateContent) {
        return internalSerialize(new EmailParameter(event, stateContent));
    }

    public static String mapEvent(InternalEvent event) {
        return internalSerialize(new EmailParameter(event));
    }

    public static String mapSession(Session session) {
        return internalSerialize(new EmailParameter(session));
    }

    public static String mapObject(Object object) {
        return internalSerialize(new EmailParameter(object));
    }

    private static String internalSerialize(EmailParameter parameter) {
        try {
            return MAPPER.writeValueAsString(parameter);
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to serialize email parameter", ex);
        }
    }
}
