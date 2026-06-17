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

package org.adealsystems.platform.orchestrator.executor.sns

import com.fasterxml.jackson.databind.ObjectMapper
import org.adealsystems.platform.orchestrator.AwsCredentialsOrchestrator
import org.adealsystems.platform.orchestrator.executor.ExecutorExitCode
import org.adealsystems.platform.orchestrator.executor.email.EmailType
import software.amazon.awssdk.services.sns.SnsClient
import software.amazon.awssdk.services.sns.model.PublishRequest
import software.amazon.awssdk.services.sns.model.PublishResponse
import spock.lang.Specification

class SnsEmailSenderSpec extends Specification {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
    private static final AwsCredentialsOrchestrator CREDENTIALS = new AwsCredentialsOrchestrator('access', 'secret', 'eu-central-1')
    private static final String TOPIC_ARN = 'arn:aws:sns:eu-central-1:123456789012:customer'

    def 'sender publishes email request as json to sns topic'() {
        given:
        def requests = []
        def client = Mock(SnsClient)
        def sender = new TestSnsEmailSender(client)

        when:
        def result = sender.sendEmail('Price report', 'The report is ready.', 'daily')

        then:
        1 * client.publish(_ as PublishRequest) >> { PublishRequest request ->
            requests << request
            PublishResponse.builder().messageId('message-1').build()
        }
        1 * client.close()
        result.result == ExecutorExitCode.SUCCESS
        result.commandId == TOPIC_ARN
        result.message == 'Message ID: message-1'

        and:
        requests.size() == 1
        requests[0].topicArn() == TOPIC_ARN
        def payload = OBJECT_MAPPER.readValue(requests[0].message(), Map)
        payload.environment == 'DEV'
        payload.type == 'INFO'
        payload.subject == 'Price report'
        payload.message == 'The report is ready.'
        payload.parameter == 'daily'
        !payload.containsKey('filename')
        !payload.containsKey('attachmentName')
    }

    def 'sender includes attachment values in sns payload'() {
        given:
        def requests = []
        def client = Mock(SnsClient)
        def sender = new TestSnsEmailSender(client)

        when:
        def result = sender.sendEmail('Attachment', 'File attached.', 'param', 'result.csv', 'Results')

        then:
        1 * client.publish(_ as PublishRequest) >> { PublishRequest request ->
            requests << request
            PublishResponse.builder().messageId('message-2').build()
        }
        1 * client.close()
        result.result == ExecutorExitCode.SUCCESS

        and:
        def payload = OBJECT_MAPPER.readValue(requests[0].message(), Map)
        payload.parameter == 'param'
        payload.filename == 'result.csv'
        payload.attachmentName == 'Results'
    }

    def 'sender replaces link token and includes link values in sns payload'() {
        given:
        def requests = []
        def client = Mock(SnsClient)
        def sender = new TestSnsEmailSender(client)

        when:
        def result = sender.sendEmailWithLink('Link', 'Open ' + SnsEmailSender.LINK_TOKEN, 's3://bucket/key.csv', 'key.csv')

        then:
        1 * client.publish(_ as PublishRequest) >> { PublishRequest request ->
            requests << request
            PublishResponse.builder().messageId('message-3').build()
        }
        1 * client.close()
        result.result == ExecutorExitCode.SUCCESS

        and:
        def payload = OBJECT_MAPPER.readValue(requests[0].message(), Map)
        payload.message == 'Open <a href="s3://bucket/key.csv">key.csv</a>'
        payload.linkTarget == 's3://bucket/key.csv'
        payload.linkName == 'key.csv'
    }

    private static class TestSnsEmailSender extends SnsEmailSender {
        private final SnsClient snsClient

        TestSnsEmailSender(SnsClient snsClient) {
            super('DEV', TOPIC_ARN, EmailType.INFO, CREDENTIALS)
            this.snsClient = snsClient
        }

        @Override
        SnsClient getSnsClient() {
            snsClient
        }
    }
}
