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
