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
import org.adealsystems.platform.io.ListDrain
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.TempDir

import java.time.LocalDateTime

@Ignore
class FileBasedEventHistorySpec extends Specification {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedEventHistorySpec)

    @TempDir
    File baseDirectory

    def 'instance works as expected'() {
        given:
        RunRepository runRepository = new RunRepositoryStub("2023-03-23", "2023-03-22")

        InternalEventClassifier eventClassifier = new InternalEventClassifier() {
            @Override
            boolean isRelevant(InternalEvent event) {
                return true
            }

            @Override
            Optional<Long> getTimeout() {
                return Optional.empty()
            }

            @Override
            boolean isSessionStartEvent(InternalEvent event) {
                return false
            }

            @Override
            boolean isSessionStopEvent(InternalEvent event, Session session) {
                return false
            }

            @Override
            Optional<String> determineDynamicContent(InternalEvent event) {
                return null
            }

            @Override
            Optional<RunSpecification> getCurrentRun() {
                return Optional.of(new RunSpecification(RunType.ACTIVE, "2023-03-23"));
            }

            @Override
            boolean isValid(InternalEvent event) {
                return true
            }
        }

        FileBasedEventHistory instance = new FileBasedEventHistory(baseDirectory, new TimestampFactoryStub(), runRepository, createObjectMapper())
        InstanceId instanceId1 = new InstanceId("0001-instance-1")
        InstanceId instanceId2 = new InstanceId("0002-instance-2")
        SessionId sessionId = new SessionId("SESSION")

        EventAffiliation rawTuple = EventAffiliation.RAW
        EventAffiliation instance1OrphanTuple = new EventAffiliation(instanceId1, null)
        EventAffiliation instance1SessionTuple = new EventAffiliation(instanceId1, sessionId)
        EventAffiliation instance2SessionTuple = new EventAffiliation(instanceId2, sessionId)
        EventAffiliation brokenTuple = new EventAffiliation(null, sessionId)

        List<InternalEvent> events = [
            createEvent("1", rawTuple),
            createEvent("2", rawTuple),
            createEvent("3", instance1OrphanTuple),
            createEvent("4", instance1OrphanTuple),
            createEvent("5", instance1SessionTuple),
            createEvent("6", instance1SessionTuple),
            createEvent("7", instance2SessionTuple),
            createEvent("8", instance2SessionTuple),
            createEvent("9", brokenTuple),
        ]
        InternalEvent singleEvent = createEvent("10", rawTuple)

        when: 'adding some events'
        instance.addAll(events)
        instance.addAll(events)
        instance.add(singleEvent)

        then: 'the files contain the expected content'
        readLines(instance, rawTuple) == [
            '{"id":"1"}',
            '{"id":"2"}',
            '{"id":"9"}',
            '{"id":"1"}',
            '{"id":"2"}',
            '{"id":"9"}',
            '{"id":"10"}',
        ]
        readLines(instance, instance1OrphanTuple) == [
            '{"id":"3","instanceId":"0001-instance-1"}',
            '{"id":"4","instanceId":"0001-instance-1"}',
            '{"id":"3","instanceId":"0001-instance-1"}',
            '{"id":"4","instanceId":"0001-instance-1"}',
        ]
        readLines(instance, instance1SessionTuple) == [
            '{"id":"5","instanceId":"0001-instance-1","sessionId":"SESSION"}',
            '{"id":"6","instanceId":"0001-instance-1","sessionId":"SESSION"}',
            '{"id":"5","instanceId":"0001-instance-1","sessionId":"SESSION"}',
            '{"id":"6","instanceId":"0001-instance-1","sessionId":"SESSION"}',
        ]
        readLines(instance, instance2SessionTuple) == [
            '{"id":"7","instanceId":"0002-instance-2","sessionId":"SESSION"}',
            '{"id":"8","instanceId":"0002-instance-2","sessionId":"SESSION"}',
            '{"id":"7","instanceId":"0002-instance-2","sessionId":"SESSION"}',
            '{"id":"8","instanceId":"0002-instance-2","sessionId":"SESSION"}',
        ]

        and: 'brokenTuple, i.e. sessionId without instanceId, is mapped to raw file'
        readLines(instance, brokenTuple) == [
            '{"id":"1"}',
            '{"id":"2"}',
            '{"id":"9"}',
            '{"id":"1"}',
            '{"id":"2"}',
            '{"id":"9"}',
            '{"id":"10"}',
        ]

        when: 'draining existing orphan events'
        ListDrain<InternalEvent> orphanDrain = new ListDrain<>()
        boolean drained = instance.drainOrphanEventsInto(eventClassifier, instance1SessionTuple, orphanDrain)

        then: 'expected events are drained and session id as expected'
        orphanDrain.content == [
            createEvent("3", instance1SessionTuple),
            createEvent("4", instance1SessionTuple),
            createEvent("3", instance1SessionTuple),
            createEvent("4", instance1SessionTuple),
        ]
        drained

        when: 'draining again'
        orphanDrain = new ListDrain<>()
        drained = instance.drainOrphanEventsInto(eventClassifier, instance1SessionTuple, orphanDrain)

        then: 'the previous events are gone'
        orphanDrain.content == []
        !drained

        when:
        instance.rollRawEvents()

        then:
        readArchiveLines(instance) == [
            '{"id":"1"}',
            '{"id":"2"}',
            '{"id":"9"}',
            '{"id":"1"}',
            '{"id":"2"}',
            '{"id":"9"}',
            '{"id":"10"}',
        ]

    }

    private static InternalEvent createEvent(String id, EventAffiliation idTuple) {
        InternalEvent result = new InternalEvent()
        result.id = id
        result.instanceId = idTuple.instanceId
        result.sessionId = idTuple.sessionId
        return result
    }

    private static List<String> readLines(FileBasedEventHistory instance, EventAffiliation idTuple) {
        File file = instance.createFile(idTuple)
        LOGGER.info("Reading lines from '{}'", file.getAbsolutePath())
        return file.readLines()
    }

    private static List<String> readArchiveLines(FileBasedEventHistory instance) {
        File rawFile = instance.createFile(EventAffiliation.RAW)
        File archive = new File(rawFile.parentFile, FileBasedEventHistory.ARCHIVE_DIRECTORY_NAME)
        File file = new File(archive, "20220217T160800128" + FileBasedEventHistory.RAW_FILE_NAME + FileBasedEventHistory.FILE_EXTENSION)
        LOGGER.info("Reading lines from '{}'", file.getAbsolutePath())
        return file.readLines()
    }

    private static createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper()
        objectMapper.registerModule(new JavaTimeModule())
        return objectMapper
    }

    static class TimestampFactoryStub implements TimestampFactory {
        @Override
        LocalDateTime createTimestamp() {
            return LocalDateTime.of(2022, 2, 17, 16, 8, 0, 128_000_000)
        }
    }
}
