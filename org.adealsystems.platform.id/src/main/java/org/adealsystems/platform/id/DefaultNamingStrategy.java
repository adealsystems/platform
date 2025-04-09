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

import org.adealsystems.platform.time.TimeHandling;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class DefaultNamingStrategy implements NamingStrategy {
    private static final char PATH_SEPARATOR = '/';
    private static final char FILE_NAME_SEPARATOR = '_';
    private static final String CURRENT = "current";

    private final DateTimeFormatter dateTimeFormatter;

    public DefaultNamingStrategy() {
        this(TimeHandling.YYYY_DOT_MM_DOT_DD_DATE_FORMATTER);
    }

    public DefaultNamingStrategy(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = Objects.requireNonNull(dateTimeFormatter, "dateTimeFormatter must not be null!");
    }

    @Override
    public String createLocalFilePart(DataInstance dataInstance) {
        StringBuilder builder = new StringBuilder();
        DataIdentifier dataIdentifier = dataInstance.getDataIdentifier();
        builder.append(dataIdentifier.getSource())
            .append(PATH_SEPARATOR);

        LocalDate date = dataInstance.getDate().orElse(null);
        if (date == null) {
            builder.append(CURRENT);
        } else {
            builder.append(date.format(dateTimeFormatter));
        }

        builder.append(PATH_SEPARATOR)
            .append(dataIdentifier.getUseCase())
            .append(PATH_SEPARATOR)
            .append(dataIdentifier.getSource())
            .append(FILE_NAME_SEPARATOR)
            .append(dataIdentifier.getUseCase());

        dataIdentifier.getConfiguration().ifPresent(t -> {
            builder.append(FILE_NAME_SEPARATOR)
                .append(t);
        });

        builder.append(dataIdentifier.getDataFormat().getExtension());

        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultNamingStrategy that = (DefaultNamingStrategy) o;
        return dateTimeFormatter.equals(that.dateTimeFormatter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateTimeFormatter);
    }

    @Override
    public String toString() {
        return "DefaultNamingStrategy{" +
            "dateTimeFormatter=" + dateTimeFormatter +
            '}';
    }
}
