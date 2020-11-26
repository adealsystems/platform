/*
 * Copyright 2020 ADEAL Systems GmbH
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

package org.adealsystems.platform.exceptions;

import java.util.Objects;

import org.adealsystems.platform.DataInstance;

public class DuplicateInstanceRegistrationException extends PlatformException {
    private static final long serialVersionUID = 1884748814399404983L;

    private final DataInstance dataInstance;

    public DuplicateInstanceRegistrationException(DataInstance dataInstance) {
        super("dataInstance " + dataInstance + " is already registered!");
        this.dataInstance = Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
    }

    public DataInstance getDataInstance() {
        return dataInstance;
    }
}
