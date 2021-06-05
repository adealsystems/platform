/*
 * Copyright 2020-2021 ADEAL Systems GmbH
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.adealsystems.platform.process.exceptions.DuplicateResolverRegistrationException;
import org.adealsystems.platform.process.exceptions.UnregisteredDataResolverException;
import org.adealsystems.platform.id.DataResolver;

public class MapDataResolverRegistry implements DataResolverRegistry {
    private final Map<DataLocation, DataResolver> dataResolvers = new HashMap<>();

    /**
     * Registers the given dataResolver for the given dataLocation.
     *
     * @param dataLocation the DataLocation
     * @param dataResolver the DataResolver
     * @throws NullPointerException                   if either dataLocation or dataResolver is null
     * @throws DuplicateResolverRegistrationException if a DataResolver is already registered for the given dataLocation
     */
    public void registerResolver(DataLocation dataLocation, DataResolver dataResolver) {
        Objects.requireNonNull(dataLocation, "dataLocation must not be null!");
        Objects.requireNonNull(dataResolver, "dataResolver must not be null!");
        DataResolver previous = dataResolvers.get(dataLocation);
        if (previous != null) {
            throw new DuplicateResolverRegistrationException(dataLocation, dataResolver);
        }
        dataResolvers.put(dataLocation, dataResolver);
    }

    /**
     * Returns the DataResolver associated with the given DataLocation
     *
     * @param dataLocation the DataLocation
     * @return the DataResolver associated with the given DataLocation
     * @throws NullPointerException              if dataLocation is null
     * @throws UnregisteredDataResolverException if no DataResolver has been registered for the given DataLocation
     */
    @Override
    public DataResolver getResolverFor(DataLocation dataLocation) {
        Objects.requireNonNull(dataLocation, "dataLocation must not be null!");
        DataResolver result = dataResolvers.get(dataLocation);
        if (result == null) {
            throw new UnregisteredDataResolverException(dataLocation);
        }
        return result;
    }

    /**
     * Returns a Set of all registered data locations.
     *
     * @return all registered data locations
     */
    @Override
    public Set<DataLocation> getLocations() {
        return Collections.unmodifiableSet(dataResolvers.keySet());
    }

    @Override
    public String toString() {
        return "MapDataResolverRegistry{" +
                "dataResolvers=" + dataResolvers +
                '}';
    }
}
