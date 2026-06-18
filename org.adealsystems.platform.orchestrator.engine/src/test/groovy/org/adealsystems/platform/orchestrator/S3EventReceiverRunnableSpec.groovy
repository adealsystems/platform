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

import spock.lang.Specification

class S3EventReceiverRunnableSpec extends Specification {

    def 'convertNotification ignores unknown S3 object fields like hasObjectAnnotation'() {
        given:
        String messageBody = '''
        {
          "Records": [
            {
              "eventName": "ObjectCreated:Put",
              "eventTime": "2026-06-18T00:12:34.000Z",
              "s3": {
                "bucket": {
                  "name": "demo-bucket"
                },
                "object": {
                  "key": "folder%2Fdemo-file.csv",
                  "size": 123,
                  "hasObjectAnnotation": true
                }
              }
            }
          ]
        }
        '''

        when:
        List<InternalEvent> events = S3EventReceiverRunnable.convertNotification(messageBody)

        then:
        events.size() == 1
        with(events.first()) {
            type == InternalEventType.FILE
            id == 'folder/demo-file.csv'
            getAttributeValue(S3Constants.BUCKET_NAME).orElse(null) == 'demo-bucket'
            getAttributeValue(S3Constants.FILE_OPERATION).orElse(null) == 'ObjectCreated:Put'
            getAttributeValue(S3Constants.FILE_SIZE).orElse(null) == '123'
            getAttributeValue(S3Constants.EVENT_TIMESTAMP).orElse(null) == '2026-06-18T00:12:34.000Z'
        }
    }

    def 'convertNotification returns empty list when Records are missing'() {
        expect:
        S3EventReceiverRunnable.convertNotification('{}').isEmpty()
    }
}
