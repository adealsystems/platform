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

package org.adealsystems.platform.orchestrator

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.adealsystems.platform.orchestrator.session.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

import java.time.LocalDateTime

class FileBasedSessionUpdateHistorySpec extends Specification {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSessionUpdateHistorySpec)

    @TempDir
    File baseDirectory

    def 'instance works as expected'() {
        given:
        FileBasedSessionUpdateHistory instance = new FileBasedSessionUpdateHistory(baseDirectory, new TimestampFactoryStub(), createObjectMapper())
        SessionId sessionId = new SessionId("SESSION")

        def setProgressMaxValueOp = new SessionSetProgressMaxValueOperation(10)
        def updateProgressOp = new SessionUpdateProgressOperation()
        def updateFailedProgressOp = new SessionUpdateFailedProgressOperation()
        def updateMessageOp = new SessionUpdateMessageOperation('message-1')

        when: 'adding some events'
        instance.add(sessionId, setProgressMaxValueOp)
        instance.add(sessionId, updateProgressOp)
        instance.add(sessionId, updateFailedProgressOp)
        instance.add(sessionId, updateMessageOp)

        then: 'the files contain the expected content'
        readLines(instance, sessionId) == [
            '{"timestamp":[2022,2,17,16,8,0,128000000],"operation":{"type":"set-progress-max-value","progressMaxValue":10}}',
            '{"timestamp":[2022,2,17,16,8,0,128000000],"operation":{"type":"update-progress"}}',
            '{"timestamp":[2022,2,17,16,8,0,128000000],"operation":{"type":"update-failed-progress"}}',
            '{"timestamp":[2022,2,17,16,8,0,128000000],"operation":{"type":"update-message","message":"message-1"}}',
        ]
//        readLines(instance, instance1OrphanTuple) == [
//            '{"id":"3","instanceId":"0001-instance-1"}',
//            '{"id":"4","instanceId":"0001-instance-1"}',
//            '{"id":"3","instanceId":"0001-instance-1"}',
//            '{"id":"4","instanceId":"0001-instance-1"}',
//        ]
//        readLines(instance, instance1SessionTuple) == [
//            '{"id":"5","instanceId":"0001-instance-1","sessionId":"SESSION"}',
//            '{"id":"6","instanceId":"0001-instance-1","sessionId":"SESSION"}',
//            '{"id":"5","instanceId":"0001-instance-1","sessionId":"SESSION"}',
//            '{"id":"6","instanceId":"0001-instance-1","sessionId":"SESSION"}',
//        ]
//        readLines(instance, instance2SessionTuple) == [
//            '{"id":"7","instanceId":"0002-instance-2","sessionId":"SESSION"}',
//            '{"id":"8","instanceId":"0002-instance-2","sessionId":"SESSION"}',
//            '{"id":"7","instanceId":"0002-instance-2","sessionId":"SESSION"}',
//            '{"id":"8","instanceId":"0002-instance-2","sessionId":"SESSION"}',
//        ]
//
//        and: 'brokenTuple, i.e. sessionId without instanceId, is mapped to raw file'
//        readLines(instance, brokenTuple) == [
//            '{"id":"1"}',
//            '{"id":"2"}',
//            '{"id":"9"}',
//            '{"id":"1"}',
//            '{"id":"2"}',
//            '{"id":"9"}',
//            '{"id":"10"}',
//        ]
//
//        when: 'draining existing orphan events'
//        ListDrain<InternalEvent> orphanDrain = new ListDrain<>()
//        boolean drained = instance.drainOrphanEventsInto(eventClassifier, instance1SessionTuple, orphanDrain)
//
//        then: 'expected events are drained and session id as expected'
//        orphanDrain.content == [
//            createEvent("3", instance1SessionTuple),
//            createEvent("4", instance1SessionTuple),
//            createEvent("3", instance1SessionTuple),
//            createEvent("4", instance1SessionTuple),
//        ]
//        drained
//
//        when: 'draining again'
//        orphanDrain = new ListDrain<>()
//        drained = instance.drainOrphanEventsInto(eventClassifier, instance1SessionTuple, orphanDrain)
//
//        then: 'the previous events are gone'
//        orphanDrain.content == []
//        !drained
    }

    private static List<String> readLines(FileBasedSessionUpdateHistory instance, SessionId sessionId) {
        File file = instance.createFile(sessionId)
        LOGGER.info("Reading lines from '{}'", file)
        return file.readLines()
    }

    private static createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper()
        objectMapper.registerModule(new JavaTimeModule())
        objectMapper.registerModule(new SessionUpdateOperationModule())
        return objectMapper
    }

    static class TimestampFactoryStub implements TimestampFactory {
        @Override
        LocalDateTime createTimestamp() {
            return LocalDateTime.of(2022, 2, 17, 16, 8, 0, 128_000_000)
        }
    }
}
