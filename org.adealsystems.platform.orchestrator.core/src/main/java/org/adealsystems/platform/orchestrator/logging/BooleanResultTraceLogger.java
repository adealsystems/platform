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

package org.adealsystems.platform.orchestrator.logging;

import org.adealsystems.platform.orchestrator.InternalEvent;
import org.slf4j.Logger;

import java.util.Objects;

public class BooleanResultTraceLogger {
    private final Logger logger;
    private final String trueMessage;
    private final String falseMessage;

    public BooleanResultTraceLogger(Logger logger, String trueMessage) {
        this(logger, trueMessage, null);
    }

    public BooleanResultTraceLogger(Logger logger, String trueMessage, String falseMessage) {
        this.logger = Objects.requireNonNull(logger, "logger must not be null!");
        this.trueMessage = trueMessage;
        this.falseMessage = falseMessage;
    }

    public boolean apply(InternalEvent event, boolean result, Object... args) {
        Objects.requireNonNull(event, "event must not be null!");

        if (result && trueMessage != null) {
            logger.trace(trueMessage, event, args);
        }
        if (!result && falseMessage != null) {
            logger.trace(falseMessage, event, args);
        }

        return result;
    }
}
