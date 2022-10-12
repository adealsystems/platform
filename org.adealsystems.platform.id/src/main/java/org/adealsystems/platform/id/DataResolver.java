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

package org.adealsystems.platform.id;

import java.time.LocalDate;
import java.util.Objects;

public final class DataResolver {
    private final DataResolutionStrategy dataResolutionStrategy;

    public DataResolver(DataResolutionStrategy dataResolutionStrategy) {
        this.dataResolutionStrategy = Objects.requireNonNull(dataResolutionStrategy, "dataResolutionStrategy must not be null!");
    }

    public DataInstance createDateInstance(DataIdentifier dataIdentifier, LocalDate date) {
        Objects.requireNonNull(date, "date must not be null!");
        return new DataInstance(dataResolutionStrategy, dataIdentifier, date);
    }

    public DataInstance createCurrentInstance(DataIdentifier dataIdentifier) {
        return new DataInstance(dataResolutionStrategy, dataIdentifier, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataResolver that = (DataResolver) o;
        return dataResolutionStrategy.equals(that.dataResolutionStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataResolutionStrategy);
    }

    @Override
    public String toString() {
        return "DataResolver{" +
            "dataResolutionStrategy=" + dataResolutionStrategy +
            '}';
    }
}
