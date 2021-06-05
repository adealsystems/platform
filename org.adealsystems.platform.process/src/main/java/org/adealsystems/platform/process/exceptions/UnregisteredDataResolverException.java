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

package org.adealsystems.platform.process.exceptions;

import org.adealsystems.platform.process.DataLocation;

import java.util.Objects;

public class UnregisteredDataResolverException extends RuntimeException {
    private static final long serialVersionUID = 2507799205666202353L;

    private final DataLocation dataLocation;

    public UnregisteredDataResolverException(DataLocation dataLocation) {
        super("No resolver defined for location '" + dataLocation + "'!");
        this.dataLocation = Objects.requireNonNull(dataLocation, "dataLocation must not be null!");
    }

    public DataLocation getDataLocation() {
        return dataLocation;
    }
}
