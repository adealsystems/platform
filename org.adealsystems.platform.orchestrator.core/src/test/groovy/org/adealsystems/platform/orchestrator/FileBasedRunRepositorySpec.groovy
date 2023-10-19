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

package org.adealsystems.platform.orchestrator


import spock.lang.Specification
import spock.lang.TempDir

class FileBasedRunRepositorySpec extends Specification {

    @TempDir
    File baseDirectory

    def 'instance works as expected'() {
        given:
        FileBasedRunRepository instance = new FileBasedRunRepository(baseDirectory)

        when:
        def activeRun = instance.retrieveActiveRun()
        def waitingRun = instance.retrieveWaitingRun()

        then:
        !activeRun.present
        !waitingRun.present

        when:
        def runId1 = "1234567"
        instance.createRun(runId1)
        activeRun = instance.retrieveActiveRun()
        waitingRun = instance.retrieveWaitingRun()

        then:
        activeRun.present
        activeRun.get() == runId1
        waitingRun.present
        waitingRun.get() == runId1

        when:
        def runId2 = "7654321"
        instance.createRun(runId2)
        activeRun = instance.retrieveActiveRun()
        waitingRun = instance.retrieveWaitingRun()

        then:
        activeRun.present
        activeRun.get() == runId2
        waitingRun.present
        waitingRun.get() == runId1

        when:
        def runId3 = "1726354"
        instance.createRun(runId3)
        activeRun = instance.retrieveActiveRun()
        waitingRun = instance.retrieveWaitingRun()

        then:
        activeRun.present
        activeRun.get() == runId3
        waitingRun.present
        waitingRun.get() == runId1

        when:
        instance.completeRun()
        activeRun = instance.retrieveActiveRun()
        waitingRun = instance.retrieveWaitingRun()

        then:
        activeRun.present
        activeRun.get() == runId3
        waitingRun.present
        waitingRun.get() == runId3

        when:
        instance.completeRun()
        activeRun = instance.retrieveActiveRun()
        waitingRun = instance.retrieveWaitingRun()

        then:
        activeRun.present
        activeRun.get() == runId3
        !waitingRun.present

        when:
        def runId4 = "453567"
        instance.createRun(runId4)
        activeRun = instance.retrieveActiveRun()
        waitingRun = instance.retrieveWaitingRun()

        then:
        activeRun.present
        activeRun.get() == runId4
        waitingRun.present
        waitingRun.get() == runId4
    }
}
