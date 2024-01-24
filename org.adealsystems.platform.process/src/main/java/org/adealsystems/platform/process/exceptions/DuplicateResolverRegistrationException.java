/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

package org.adealsystems.platform.process.exceptions;

import org.adealsystems.platform.id.DataResolver;
import org.adealsystems.platform.process.DataLocation;

import java.util.Objects;


public class DuplicateResolverRegistrationException extends RuntimeException {
    private static final long serialVersionUID = -2561489709678465409L;

    private final DataLocation dataLocation;
    private final DataResolver dataResolver;

    public DuplicateResolverRegistrationException(DataLocation dataLocation, DataResolver dataResolver) {
        super("location '" + dataLocation + "' does already have resolver " + dataResolver + "!");
        this.dataLocation = Objects.requireNonNull(dataLocation, "dataInstance must not be null!");
        this.dataResolver = Objects.requireNonNull(dataResolver, "dataResolver must not be null!");
    }

    public DataLocation getDataLocation() {
        return dataLocation;
    }

    public DataResolver getDataResolver() {
        return dataResolver;
    }
}
