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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileBasedInstanceRepository implements InstanceRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedInstanceRepository.class);

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        OBJECT_MAPPER.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    }

    private static final Pattern FILE_PATTERN = Pattern.compile('(' + InstanceId.PATTERN_STRING + ")\\.json");

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final File baseDirectory;

    public FileBasedInstanceRepository(File baseDirectory) {
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
    public Optional<InstanceId> findInstanceId(String id) {
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            File[] allFiles = baseDirectory.listFiles();
            if (allFiles == null) {
                return Optional.empty();
            }

            Set<InstanceId> allMatchings = Arrays.stream(allFiles)
                .map(file -> {
                    Matcher matcher = FILE_PATTERN.matcher(file.getName());
                    if (!matcher.matches()) {
                        return null;
                    }
                    String currentId = matcher.group(1);
                    if (!id.equals(currentId)) {
                        return null;
                    }
                    return new InstanceId(currentId);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

            return allMatchings.stream().findFirst();
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<InstanceId> retrieveInstanceIds() {
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            File[] allFiles = baseDirectory.listFiles();
            if (allFiles == null) {
                return Collections.emptySet();
            }

            return Arrays.stream(allFiles)
                .map(file -> {
                    Matcher matcher = FILE_PATTERN.matcher(file.getName());
                    if (!matcher.matches()) {
                        return null;
                    }
                    return new InstanceId(matcher.group(1));
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public Instance createInstance(InstanceId id) {
        File instanceFile = getInstanceFile(id);

        Instance instance = new Instance(id);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (instanceFile.exists()) {
                throw new IllegalArgumentException("Instance with id '" + id + "' already exists!");
            }

            writeInstance(instanceFile, instance);
        }
        finally {
            writeLock.unlock();
        }

        return instance;
    }

    @Override
    public Optional<Instance> retrieveInstance(InstanceId id) {
        File instanceFile = getInstanceFile(id);

        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        readLock.lock();
        try {
            if (!instanceFile.exists()) {
                return Optional.empty();
            }

            return Optional.of(readInstance(instanceFile, id));
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public Instance retrieveOrCreateInstance(InstanceId id) {
        File instanceFile = getInstanceFile(id);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (instanceFile.exists()) {
                return readInstance(instanceFile, id);
            }

            Instance instance = new Instance(id);
            writeInstance(instanceFile, instance);
            return instance;
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public void updateInstance(Instance instance) {
        Objects.requireNonNull(instance, "instance must not be null!");

        InstanceId id = instance.getId();
        File instanceFile = getInstanceFile(id);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            if (!instanceFile.exists()) {
                throw new IllegalArgumentException("Instance with id '" + id + "' does not exist!");
            }

            writeInstance(instanceFile, instance);
        }
        finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean deleteInstance(InstanceId id) {
        File instanceFile = getInstanceFile(id);

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            return instanceFile.delete();
        }
        finally {
            writeLock.unlock();
        }
    }

    File getInstanceFile(InstanceId id) {
        Objects.requireNonNull(id, "id must not be null!");
        return new File(baseDirectory, id.getId() + ".json");
    }

    private Instance readInstance(File instanceFile, InstanceId id) {
        try {
            Instance stored = OBJECT_MAPPER.readValue(instanceFile, Instance.class);
            InstanceId storedId = stored.getId();
            if (storedId.equals(id)) {
                return stored;
            }

            LOGGER.warn(
                "InstanceID {} stored in {} does not match requested ID {}! Overriding it!",
                storedId,
                instanceFile,
                id
            );
            Instance result = new Instance(id);
            result.setConfiguration(stored.getConfiguration());
            return result;
        }
        catch (IOException ex) {
            throw new IllegalStateException("Unable to read instance file '" + instanceFile + "'!", ex);
        }
    }

    private void writeInstance(File instanceFile, Instance instance) {
        try {
            OBJECT_MAPPER.writeValue(instanceFile, instance);
        }
        catch (IOException ex) {
            throw new IllegalStateException("Unable to write instance file '" + instanceFile + "'!", ex);
        }
    }
}
