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

package org.adealsystems.platform.orchestrator.executor.email;


import org.adealsystems.platform.orchestrator.InternalEvent;
import org.adealsystems.platform.orchestrator.Session;

public class EmailParameter {

    private InternalEvent event;
    private Object stateContent;
    private Session session;
    private Object object;

    public EmailParameter(InternalEvent event, Object stateContent) {
        this.event = event;
        this.stateContent = stateContent;
    }

    public EmailParameter(Object object) {
        this.object = object;
    }

    public EmailParameter(InternalEvent event) {
        this.event = event;
    }

    public EmailParameter(Session session) {
        this.session = session;
    }

    public InternalEvent getEvent() {
        return event;
    }

    public void setEvent(InternalEvent event) {
        this.event = event;
    }

    public Object getStateContent() {
        return stateContent;
    }

    public void setStateContent(Object stateContent) {
        this.stateContent = stateContent;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }
}
