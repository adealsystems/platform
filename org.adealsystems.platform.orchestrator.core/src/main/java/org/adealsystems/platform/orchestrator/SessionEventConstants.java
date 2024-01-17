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

package org.adealsystems.platform.orchestrator;

public interface SessionEventConstants {
    String SESSION_START = "SESSION_START";
    String SESSION_RESUME = "SESSION_RESUME";
    String SESSION_STOP = "SESSION_STOP";
    String SESSION_LIFECYCLE = "SESSION_LIFECYCLE";
    String SESSION_STATE = "SESSION_STATE";

    String RUN_ID_ATTRIBUTE_NAME = "__run_id";
    String INSTANCE_ID_ATTRIBUTE_NAME = "__instance_id";
    String SESSION_ID_ATTRIBUTE_NAME = "__session_id";
    String SESSION_LIFECYCLE_ATTRIBUTE_NAME = "__session_lifecycle";
    String SESSION_STATE_ATTRIBUTE_NAME = "__session_state";
    String SOURCE_EVENT_ATTRIBUTE_NAME = "__source_event";
    String DYNAMIC_CONTENT_ATTRIBUTE_NAME = "__dynamic_content";
    String HANDLED_EVENTS_ATTRIBUTE_NAME = "__handled_events";
    String SESSION_NAME = "__session_name";

    String TERMINATING_FLAG = "__terminating";

    // String UNASSIGNED_COMMAND_IDS = "__unassigned_command_ids";
}
