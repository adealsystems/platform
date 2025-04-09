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

class FileBasedActiveSessionIdRepositorySpec extends Specification {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedActiveSessionIdRepositorySpec)

    @TempDir
    File baseDirectory

    def 'instance works as expected'() {
        given:
        InstanceId instanceId = new InstanceId('0001-instance-id')
        def sessionIdGenerator = new SessionIdGeneratorMock()

        FileBasedActiveSessionIdRepository instance = new FileBasedActiveSessionIdRepository(sessionIdGenerator, baseDirectory)

        when:
        def s = instance.createActiveSessionId(instanceId)

        then:
        s != null
        s.present
        s.get().id == 'SESSION-ID-1'

        when:
        def otherS = instance.retrieveActiveSessionId(instanceId)

        then:
        otherS != null
        otherS.present
        otherS.get().id == 'SESSION-ID-1'

        when:
        def lines = readLines(instance, instanceId)

        then:
        lines == ['{"sessionId":"SESSION-ID-1"}']

        when:
        def existingS = instance.retrieveOrCreateActiveSessionId(instanceId)

        then:
        existingS != null
        existingS.id == 'SESSION-ID-1'

        when:
        def deleted = instance.deleteActiveSessionId(instanceId)

        then:
        deleted

        when:
        def oneMoreS = instance.retrieveOrCreateActiveSessionId(instanceId)

        then:
        oneMoreS.id == 'SESSION-ID-2'
    }

    def 'list all instances works as expected'() {
        given:
        InstanceId instanceId = new InstanceId('0001-instance-id')
        def sessionIdGenerator = new SessionIdGeneratorMock()

        FileBasedActiveSessionIdRepository repository = new FileBasedActiveSessionIdRepository(sessionIdGenerator, baseDirectory)

        when:
        def all = repository.listAllActiveInstances()

        then:
        all != null
        all.isEmpty()

        when:
        def s = repository.createActiveSessionId(instanceId)
        all = repository.listAllActiveInstances()

        then:
        all != null
        all.size() == 1
        instanceId == all[0]

        when:
        def deleted = repository.deleteActiveSessionId(instanceId)

        then:
        deleted
        repository.listAllActiveInstances().isEmpty()
    }

    static class SessionIdGeneratorMock implements SessionIdGenerator {
        int counter = 0

        @Override
        SessionId generate() {
            counter++
            return new SessionId('SESSION-ID-' + counter)
        }
    }

    private static List<String> readLines(FileBasedActiveSessionIdRepository instance, InstanceId id) {
        File file = instance.getInstanceFile(id)
        LOGGER.info("Reading lines from '{}'", file.getAbsolutePath())
        return file.readLines()
    }
}
