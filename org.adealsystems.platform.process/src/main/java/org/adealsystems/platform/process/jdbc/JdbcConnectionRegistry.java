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

package org.adealsystems.platform.process.jdbc;

import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.process.exceptions.DuplicateIdentifierRegistrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class JdbcConnectionRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnectionRegistry.class);

    private final Map<DataIdentifier, JdbcConnectionProperties> jdbcPropertiesMapping = new HashMap<>();

    /**
     * Registers the given jdbc connection properties.
     *
     * @param dataIdentifier the data instance.
     * @param props jdbc connection properties
     * @throws NullPointerException                   if the given data identifier is null
     * @throws DuplicateIdentifierRegistrationException if the data instance was already registered
     */
    public void register(DataIdentifier dataIdentifier, JdbcConnectionProperties props) {
        Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");
        JdbcConnectionProperties previous = jdbcPropertiesMapping.put(dataIdentifier, props);
        if (previous != null) {
            throw new DuplicateIdentifierRegistrationException(dataIdentifier);
        }
    }

    /**
     * Resolves a jdbc connection properties for the given data identifier.
     *
     * @param dataIdentifier the data identifier
     * @return the JdbcConnectionProperties associated with the given data identifier or Optional.empty() if no properties was registered.
     * @throws NullPointerException if dataIdentifier is null
     */
    public Optional<JdbcConnectionProperties> resolve(DataIdentifier dataIdentifier) {
        Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");
        JdbcConnectionProperties props = jdbcPropertiesMapping.get(dataIdentifier);
        if (props == null) {
            LOGGER.warn("dataIdentifier {} has not been registered!", dataIdentifier);
            return Optional.empty();
        }

        return Optional.of(props);
    }

    public void clear() {
        jdbcPropertiesMapping.clear();
    }
}
