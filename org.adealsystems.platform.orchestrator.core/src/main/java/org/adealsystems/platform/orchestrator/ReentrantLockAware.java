package org.adealsystems.platform.orchestrator;

import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public interface ReentrantLockAware {
    Map<String, ReentrantLock> getLocks();
}
