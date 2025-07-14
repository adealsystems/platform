package org.adealsystems.platform.orchestrator.handler;

import java.util.Map;

public interface ConfigurableHandler {
    void configure(String instanceKey, Map<String, String> config);
}
