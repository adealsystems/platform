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
