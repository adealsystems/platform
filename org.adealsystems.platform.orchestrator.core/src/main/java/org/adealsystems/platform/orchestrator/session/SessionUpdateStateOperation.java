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

package org.adealsystems.platform.orchestrator.session;

import org.adealsystems.platform.orchestrator.Session;
import org.adealsystems.platform.orchestrator.status.State;

import java.util.Objects;

public class SessionUpdateStateOperation implements SessionUpdateOperation {
    private final State state;

    public SessionUpdateStateOperation(State state) {
        this.state = state;
    }

    @Override
    public void apply(Session session) {
        Objects.requireNonNull(session, "session must not be null!");

        session.getProcessingState().setState(state);
    }

    public State getState() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SessionUpdateStateOperation that = (SessionUpdateStateOperation) o;
        return state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(state);
    }

    @Override
    public String toString() {
        return "SessionUpdateStateOperation{" +
            "state=" + state +
            '}';
    }
}
