/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

package org.adealsystems.platform.process;

import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.id.DataInstance;
import org.adealsystems.platform.process.exceptions.DuplicateInstanceRegistrationException;
import org.adealsystems.platform.process.exceptions.DuplicateUniqueIdentifierException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class DataInstanceRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataInstanceRegistry.class);

    private final Map<DataIdentifier, DataInstanceContainer> dataInstanceContainers = new HashMap<>();

    /**
     * Registers the given data instance.
     *
     * @param dataInstance the data instance.
     * @throws NullPointerException                   if the given data instance is null
     * @throws DuplicateInstanceRegistrationException if the data instance was already registered
     */
    public void register(DataInstance dataInstance) {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        DataIdentifier dataIdentifier = dataInstance.getDataIdentifier();
        DataInstanceContainer container = dataInstanceContainers.get(dataIdentifier);
        if (container == null) {
            container = new DataInstanceContainer();
            dataInstanceContainers.put(dataIdentifier, container);
        }
        container.register(dataInstance);
    }

    /**
     * Resolves a unique data instance for the given data identifier.
     * <p>
     * This method throws an exception if more than one data instance was registered for the given data identifier.
     *
     * @param dataIdentifier the data identifier
     * @return the unique DataInstance associated with the given data identifier or Optional.empty() if no instance was registered.
     * @throws NullPointerException if dataIdentifier is null
     */
    public Optional<DataInstance> resolveUnique(DataIdentifier dataIdentifier) {
        Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");
        DataInstanceContainer container = dataInstanceContainers.get(dataIdentifier);
        if (container == null) {
            LOGGER.warn("dataIdentifier {} has not been registered!", dataIdentifier);
            return Optional.empty();
        }

        DataInstance currentInstance = container.getCurrent();
        Map<LocalDate, DataInstance> dateInstances = container.getDateInstances();

        if (currentInstance == null) {
            int size = dateInstances.size();
            if (size == 1) {
                return Optional.of(dateInstances.entrySet().stream().findFirst().get().getValue());
            }
            if (size == 0) {
                return Optional.empty();
            }
            throw new DuplicateUniqueIdentifierException(dataIdentifier);
        }

        if (dateInstances.isEmpty()) {
            return Optional.of(currentInstance);
        }
        throw new DuplicateUniqueIdentifierException(dataIdentifier);
    }

    /**
     * Returns a Set of all data instances registered for the given data identifier.
     *
     * @param dataIdentifier the DataIdentifier
     * @return a Set of all data instances registered for the given data identifier.
     * @throws NullPointerException if dataIdentifier is null
     */
    public Set<DataInstance> resolveAll(DataIdentifier dataIdentifier) {
        Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");
        Set<DataInstance> result = new HashSet<>();

        DataInstanceContainer container = dataInstanceContainers.get(dataIdentifier);
        if (container == null) {
            LOGGER.warn("dataIdentifier {} has not been registered!", dataIdentifier);
            return result;
        }

        DataInstance currentInstance = container.getCurrent();
        if (currentInstance != null) {
            result.add(currentInstance);
        }
        for (Map.Entry<LocalDate, DataInstance> entry : container.getDateInstances().entrySet()) {
            result.add(entry.getValue());
        }
        return result;
    }

    /**
     * Returns a Set of all registered DataIdentifiers.
     *
     * @return a Set of all registered DataIdentifiers.
     */
    public Set<DataIdentifier> allIdentifier() {
        return new HashSet<>(dataInstanceContainers.keySet());
    }

    private static class DataInstanceContainer {
        private final Map<LocalDate, DataInstance> dateInstances = new HashMap<>();
        private DataInstance current;

        public void register(DataInstance dataInstance) {
            LocalDate date = dataInstance.getDate().orElse(null);
            if (date == null) {
                if (current != null) {
                    throw new DuplicateInstanceRegistrationException(dataInstance);
                }
                current = dataInstance;
                return;
            }

            if (dateInstances.containsKey(date)) {
                throw new DuplicateInstanceRegistrationException(dataInstance);
            }
            dateInstances.put(date, dataInstance);
        }

        public Map<LocalDate, DataInstance> getDateInstances() {
            return dateInstances;
        }

        public DataInstance getCurrent() {
            return current;
        }
    }

    // TODO: ??? really?
    public void clear() {
        dataInstanceContainers.clear();
    }
}
