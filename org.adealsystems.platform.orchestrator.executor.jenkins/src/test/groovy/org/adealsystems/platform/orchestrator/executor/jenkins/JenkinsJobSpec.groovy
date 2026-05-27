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

class JenkinsJobSpec extends Specification {

    private static final String JENKINS_URL = 'https://jenkins.example'
    private static final String JENKINS_USERNAME = 'user'
    private static final String JENKINS_TOKEN = 'token'
    private static final DataIdentifier DATA_ID = new DataIdentifier('source', 'usecase', DataFormat.JSON)

    def 'job creates buildWithParameters command with additional parameters and command id'() {
        given:
        def job = new TestJenkinsJob()

        when:
        def command = job.createCommand('command-1', DATA_ID)

        then:
        command == ('curl -X POST --user user:token https://jenkins.example/job/test-job/buildWithParameters'
            + ' --data task_version=1.2.0'
            + ' --data jar_version=2.5.1'
            + ' --data commandId=command-1')
    }

    def 'job creates build command when there are no parameters'() {
        given:
        def job = new EmptyJenkinsJob()

        when:
        def command = job.createCommand(null, DATA_ID)

        then:
        command == 'curl -X POST --user user:token https://jenkins.example/job/test-job/build'
    }

    def 'job uses custom command id parameter name'() {
        given:
        def job = new EmptyJenkinsJob()
        job.setCommandIdParamName('COMMAND_ID')

        when:
        def command = job.createCommand('command-1', DATA_ID)

        then:
        command == ('curl -X POST --user user:token https://jenkins.example/job/test-job/buildWithParameters'
            + ' --data COMMAND_ID=command-1')
    }

    private static class TestJenkinsJob extends JenkinsJob {
        TestJenkinsJob() {
            super(JENKINS_URL, JENKINS_USERNAME, JENKINS_TOKEN)
        }

        @Override
        protected Map<String, String> prepareAdditionalParameters(DataIdentifier dataIdentifier) {
            [
                task_version: '1.2.0',
                jar_version : '2.5.1'
            ]
        }

        @Override
        protected String getJenkinsJobName(Map<String, String> additionalParameters) {
            'test-job'
        }
    }

    private static class EmptyJenkinsJob extends JenkinsJob {
        EmptyJenkinsJob() {
            super(JENKINS_URL, JENKINS_USERNAME, JENKINS_TOKEN)
        }

        @Override
        protected Map<String, String> prepareAdditionalParameters(DataIdentifier dataIdentifier) {
            [:]
        }

        @Override
        protected String getJenkinsJobName(Map<String, String> additionalParameters) {
            'test-job'
        }
    }
}
