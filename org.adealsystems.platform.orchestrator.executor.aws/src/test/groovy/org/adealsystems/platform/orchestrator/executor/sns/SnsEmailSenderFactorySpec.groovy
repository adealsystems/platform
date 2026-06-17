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

import org.adealsystems.platform.orchestrator.AwsCredentialsOrchestrator
import org.adealsystems.platform.orchestrator.executor.email.EmailType
import org.adealsystems.platform.orchestrator.executor.email.RecipientsCluster
import spock.lang.Specification

class SnsEmailSenderFactorySpec extends Specification {

    private static final AwsCredentialsOrchestrator CREDENTIALS = new AwsCredentialsOrchestrator('access', 'secret', 'eu-central-1')
    private static final String INTERNAL_TOPIC_ARN = 'arn:aws:sns:eu-central-1:123456789012:internal'
    private static final String PROJECT_TOPIC_ARN = 'arn:aws:sns:eu-central-1:123456789012:project'

    def 'factory selects sns topic by recipients cluster'() {
        given:
        def factory = new SnsEmailSenderFactory(
            CREDENTIALS,
            [
                (RecipientsCluster.INTERNAL): INTERNAL_TOPIC_ARN,
                (RecipientsCluster.PROJECT) : PROJECT_TOPIC_ARN
            ]
        )

        when:
        def sender = factory.getSender('DEV', RecipientsCluster.PROJECT, EmailType.ERROR)

        then:
        sender instanceof SnsEmailSender
        sender.topicArn == PROJECT_TOPIC_ARN
    }

    def 'factory rejects unmapped recipients cluster'() {
        given:
        def factory = new SnsEmailSenderFactory(CREDENTIALS, [(RecipientsCluster.INTERNAL): INTERNAL_TOPIC_ARN])

        when:
        factory.getSender('DEV', RecipientsCluster.CUSTOMER, EmailType.INFO)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message == "Unknown/unsupported recipients cluster 'CUSTOMER'!"
    }
}
