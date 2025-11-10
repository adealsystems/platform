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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class FileBasedSessionRepositoryFactory implements SessionRepositoryFactory, ReentrantLockAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSessionRepositoryFactory.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final ConcurrentMap<InstanceId, SessionRepository> cache = new ConcurrentHashMap<>();

    private final File baseDirectory;

    public FileBasedSessionRepositoryFactory(File baseDirectory) {
        Objects.requireNonNull(baseDirectory, "baseDirectory must not be null!");
        if (!baseDirectory.exists()) {
            throw new IllegalArgumentException("Missing mandatory baseDirectory: '" + baseDirectory + "'!");
        }
        if (!baseDirectory.isDirectory()) {
            throw new IllegalArgumentException("baseDirectory '" + baseDirectory + "' must be directory!");
        }
        this.baseDirectory = baseDirectory;
    }

    @Override
    public Map<String, ReentrantLock> getLocks() {
        return Map.of("lock", lock);
    }

    @Override
    public SessionRepository retrieveSessionRepository(InstanceId instanceId) {
        Objects.requireNonNull(instanceId, "instanceId must not be null!");

        lock.lock();
        try {
            SessionRepository repo = cache.get(instanceId);
            if (repo != null) {
                return repo;
            }

            repo = createRepository(instanceId);
            cache.put(instanceId, repo);
            return repo;
        } finally {
            lock.unlock();
        }
    }

    private SessionRepository createRepository(InstanceId id) {
        File instanceDirectory = new File(baseDirectory, id.getId());
        if (!instanceDirectory.mkdirs()) {
            if (!instanceDirectory.isDirectory()) {
                throw new IllegalStateException("Failed to create base directory '" + instanceDirectory + "'!");
            }
        } else {
            LOGGER.info("Created new base directory '{}'", instanceDirectory);
        }

        return new SynchronizedFileBasedSessionRepository(id, instanceDirectory);
    }
}
