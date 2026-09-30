/*
 * Copyright 2020-2026 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator.session


import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification
import tools.jackson.databind.json.JsonMapper

import java.time.LocalDateTime

class SessionUpdateTimestampOperationSpec extends Specification {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionUpdateTimestampOperationSpec.class)
    private static final JsonMapper JSON_MAPPER =
        JsonMapper.builder()
            .build();

    def serializationTest() {
        given:
        SessionUpdateTimestampOperation subject = new SessionUpdateTimestampOperation(
            SessionTimestamp.STARTED,
            LocalDateTime.now()
        );

        when:
        String json =
            JSON_MAPPER.writeValueAsString(subject);
        LOGGER.info("JSON: {}", json);
        var parsed =
            JSON_MAPPER.readValue(json, SessionUpdateTimestampOperation.class)

        then:
        subject == parsed
    }
}
