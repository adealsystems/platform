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

import org.adealsystems.platform.id.DataIdentifier;

import java.util.Objects;

public class DuplicateUniqueIdentifierException extends RuntimeException {
    private static final long serialVersionUID = 4127296498700462187L;
    private final DataIdentifier dataIdentifier;

    public DuplicateUniqueIdentifierException(DataIdentifier dataIdentifier) {
        super("Multiple data instances for dataIdentifier " + dataIdentifier + "!");
        this.dataIdentifier = Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");
    }

    public DataIdentifier getDataIdentifier() {
        return dataIdentifier;
    }
}
