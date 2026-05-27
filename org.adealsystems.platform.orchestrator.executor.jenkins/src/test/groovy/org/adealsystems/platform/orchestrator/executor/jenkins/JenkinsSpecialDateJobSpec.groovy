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

package org.adealsystems.platform.orchestrator.executor.jenkins

import org.adealsystems.platform.id.DataFormat
import org.adealsystems.platform.id.DataIdentifier
import spock.lang.Specification

import java.time.LocalDate

class JenkinsSpecialDateJobSpec extends Specification {

    private static final String JENKINS_URL = 'https://jenkins.example'
    private static final String JENKINS_USERNAME = 'user'
    private static final String JENKINS_TOKEN = 'token'
    private static final DataIdentifier DATA_ID = new DataIdentifier('source', 'usecase', DataFormat.JSON)

    def 'special date job creates command with input date and command id'() {
        given:
        def job = new TestJenkinsSpecialDateJob()

        when:
        def command = job.createCommand('command-1', LocalDate.of(2026, 5, 27), DATA_ID)

        then:
        command == ('curl -X POST --user user:token https://jenkins.example/job/special-date-job/buildWithParameters'
            + ' --data INPUTDATE=2026-05-27'
            + ' --data job=source:usecase:JSON'
            + ' --data commandId=command-1')
    }

    private static class TestJenkinsSpecialDateJob extends JenkinsSpecialDateJob {
        TestJenkinsSpecialDateJob() {
            super(JENKINS_URL, JENKINS_USERNAME, JENKINS_TOKEN)
        }

        @Override
        protected Map<String, String> prepareAdditionalParameters(LocalDate inputDate, DataIdentifier dataIdentifier) {
            [
                INPUTDATE: inputDate.toString(),
                job      : dataIdentifier.toString()
            ]
        }

        @Override
        protected String getJenkinsJobName(Map<String, String> additionalParameters) {
            'special-date-job'
        }
    }
}
