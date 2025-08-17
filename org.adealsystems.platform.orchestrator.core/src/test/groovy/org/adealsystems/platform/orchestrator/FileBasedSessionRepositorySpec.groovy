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


import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification
import spock.lang.TempDir

import java.time.LocalDateTime

class FileBasedSessionRepositorySpec extends Specification {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSessionRepositorySpec)

    @TempDir
    File baseDirectory
    def 'instance works as expected'() {
        given:
        InstanceId instanceId = new InstanceId('0001-instance-id')
        def instanceConfiguration = [
            'aaa': 'xxx',
            'bbb': 'yyy'
        ]
        def sessionState = [
            'ccc': 'zzz',
            'ddd': '000',
        ]

        FileBasedSessionRepository sessionRepository = new FileBasedSessionRepository(instanceId, baseDirectory, null)
        def id = 'SESSION-ID'
        SessionId sessionId = new SessionId(id)

        when:
        def allIds = sessionRepository.retrieveSessionIds()

        then:
        allIds == [] as Set

        when:
        def session = sessionRepository.createSession(sessionId)

        then:
        session != null
        session.id == sessionId
        session.instanceConfiguration == [:]
        session.state == null

        when:
        allIds = sessionRepository.retrieveSessionIds()

        then:
        allIds == [sessionId] as Set

        when:
        def sessionWithConfig = new Session(instanceId, session.getId(), LocalDateTime.of(2023, 3,24,0,0,0,0), instanceConfiguration)
        sessionWithConfig.setState(sessionState)
        sessionRepository.updateSession(sessionWithConfig)

        def otherSession = sessionRepository.retrieveSession(sessionId)

        then:
        otherSession.present
        otherSession.get().id == sessionId
        otherSession.get().instanceConfiguration == instanceConfiguration
        otherSession.get().state == sessionState

        when:
        def lines = readLines(sessionRepository, sessionId)

        then:
        lines == [
            '{',
            '  "instanceId" : "0001-instance-id",',
            '  "id" : "SESSION-ID",',
            '  "creationTimestamp" : [ 2023, 3, 24, 0, 0 ],',
            '  "instanceConfiguration" : {',
            '    "aaa" : "xxx",',
            '    "bbb" : "yyy"',
            '  },',
            '  "state" : {',
            '    "ccc" : "zzz",',
            '    "ddd" : "000"',
            '  }',
            '}'
        ]

        when:
        def existingSession = sessionRepository.retrieveOrCreateSession(sessionId)

        then:
        existingSession.id == sessionId
        existingSession.instanceConfiguration == instanceConfiguration
        existingSession.state == sessionState

        when:
        def deleted = sessionRepository.deleteSession(sessionId)

        then:
        deleted

        when:
        def oneMoreSession = sessionRepository.retrieveSession(sessionId)

        then:
        !oneMoreSession.present

        when:
        def freshSession = sessionRepository.retrieveOrCreateSession(sessionId)

        then:
        freshSession.id == sessionId
        freshSession.instanceConfiguration == [:]
        freshSession.state == null
    }

    private static List<String> readLines(FileBasedSessionRepository instance, SessionId id) {
        File file = instance.getSessionFile(id)
        LOGGER.info("Reading lines from '{}'", file.getAbsolutePath())
        return file.readLines()
    }
}
