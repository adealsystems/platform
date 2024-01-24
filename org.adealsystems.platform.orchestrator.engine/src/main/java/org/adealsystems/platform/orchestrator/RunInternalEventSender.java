/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

package org.adealsystems.platform.orchestrator;

import software.amazon.awssdk.services.sqs.SqsClient;

import static org.adealsystems.platform.orchestrator.InternalEvent.ATTR_RUN_ID;


public class RunInternalEventSender extends QueueInternalEventSender {

    public RunInternalEventSender(String queueName, SqsClient sqsClient) {
        super(queueName, sqsClient);
    }

    @Override
    public void sendEvent(InternalEvent event) {
        throw new UnsupportedOperationException("Calling sendEvent(InternalEvent) directly is not allowed!");
    }

    public void sendEvent(String eventId, String runId) {
        InternalEvent event = new InternalEvent();
        event.setType(InternalEventType.RUN);
        event.setId(eventId);
        event.setAttributeValue(ATTR_RUN_ID, runId);
        super.sendEvent(event);
    }
}
