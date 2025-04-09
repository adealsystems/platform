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

package org.adealsystems.platform.id;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;

public final class DataInstance implements Comparable<DataInstance> {

    private final DataIdentifier dataIdentifier;
    private final LocalDate date;
    private final DataResolutionStrategy dataResolutionStrategy;

    public DataInstance(DataResolutionStrategy dataResolutionStrategy, DataIdentifier dataIdentifier, LocalDate date) {
        this.dataResolutionStrategy = Objects.requireNonNull(dataResolutionStrategy, "dataResolutionStrategy must not be null!");
        this.dataIdentifier = Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");
        this.date = date;
    }

    public DataIdentifier getDataIdentifier() {
        return dataIdentifier;
    }

    public DataFormat getDataFormat() {
        return dataIdentifier.getDataFormat();
    }

    public Optional<LocalDate> getDate() {
        return Optional.ofNullable(date);
    }

    public String getPath() {
        return dataResolutionStrategy.getPath(this);
    }

    public OutputStream getOutputStream() throws IOException {
        return dataResolutionStrategy.getOutputStream(this);
    }

    public InputStream getInputStream() throws IOException {
        return dataResolutionStrategy.getInputStream(this);
    }

    public boolean delete() throws IOException {
        return dataResolutionStrategy.delete(this);
    }

    public boolean supports(DataResolutionCapability capability) {
        return dataResolutionStrategy.supports(capability);
    }

    public DataResolutionStrategy getDataResolutionStrategy() {
        return dataResolutionStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataInstance that = (DataInstance) o;
        return dataIdentifier.equals(that.dataIdentifier) &&
            Objects.equals(date, that.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataIdentifier, date);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(dataIdentifier).append('#');
        if (date == null) {
            builder.append("current");
        } else {
            builder.append(date);
        }
        return builder.toString();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(DataInstance other) {
        Objects.requireNonNull(other, "other must not be null!");
        int result = dataIdentifier.compareTo(other.dataIdentifier);
        if (result != 0) {
            return result;
        }
        if (date == null) {
            if (other.date == null) {
                return 0;
            }
            return -1;
        }
        if (other.date == null) {
            return 1;
        }
        return date.compareTo(other.date);
    }
}
