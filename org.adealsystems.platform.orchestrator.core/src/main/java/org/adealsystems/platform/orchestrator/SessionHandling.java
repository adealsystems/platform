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

package org.adealsystems.platform.orchestrator;

import java.util.Map;
import java.util.Set;

public final class SessionHandling {
    public static final String ATTR_STEPS_IN_PROGRESS_PREFIX = "step-in-progress:";
    public static final String ATTR_STEPS_IN_PROGRESS = "steps-in-progress";
    public static final String ATTR_COMMANDS_IN_PROGRESS = "commands-in-progress";
    public static final String ATTR_COMPLETED_STEPS = "completed-steps";
    public static final String ATTR_COMPLETED_COMMANDS = "completed-commands";

    private SessionHandling() {
    }

    public static void registerCommand(Session session, String commandId, String step) {
        session.setStateValue(ATTR_STEPS_IN_PROGRESS_PREFIX + commandId, step);
        session.extendStateRegistry(ATTR_COMMANDS_IN_PROGRESS, commandId);
        session.extendStateRegistry(ATTR_STEPS_IN_PROGRESS, step);
    }

    public static Set<String> registerCommandCompleteness(Session session, String stepName) {
        Set<String> completedCommands = session.extendStateRegistry(ATTR_COMPLETED_STEPS, stepName);

        session.reduceStateRegistry(ATTR_STEPS_IN_PROGRESS, stepName);

        // search for running steps
        Map<String, String> state = session.getState();
        String reference = null;
        for (Map.Entry<String, String> entry : state.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (stepName.equals(value) && key.startsWith(ATTR_STEPS_IN_PROGRESS_PREFIX)) {
                reference = key;
                break;
            }
        }

        if (reference != null) {
            session.setStateValue(reference, null);
            String commandId = reference.substring(ATTR_STEPS_IN_PROGRESS_PREFIX.length());
            session.reduceStateRegistry(ATTR_COMMANDS_IN_PROGRESS, commandId);
            session.extendStateRegistry(ATTR_COMPLETED_COMMANDS, commandId);
        }

        return completedCommands;
    }
}
