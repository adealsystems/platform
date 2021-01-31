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

package org.adealsystems.platform;

import java.util.Objects;
import java.util.Optional;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public final class DataIdentifier implements Comparable<DataIdentifier> {
    public static final String SEPARATOR = ":";
    static final String PATTERN_STRING = "[a-z][0-9a-z]*(_[0-9a-z]+)*";
    private static final Pattern PATTERN = Pattern.compile(PATTERN_STRING);

    private final String source;
    private final String useCase; // only available in the curated zone
    private final String configuration;
    private final DataFormat dataFormat;

    public DataIdentifier(String source, String useCase, DataFormat dataFormat) {
        this(source, useCase, null, dataFormat);
    }

    public DataIdentifier(String source, String useCase, String configuration, DataFormat dataFormat) {
        this.source = checkElement("source", source, false);
        this.useCase = checkElement("useCase", useCase, false);
        this.configuration = checkElement("configuration", configuration, true);
        this.dataFormat = Objects.requireNonNull(dataFormat, "dataFormat must not be null!");
    }

    static String checkElement(String name, String value, boolean optional) {
        Objects.requireNonNull(name, "name must not be null!");
        if (optional && value == null) {
            return null;
        }

        Objects.requireNonNull(value, name + " must not be null!");
        if (!PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " value '" + value + "' doesn't match the pattern '" + PATTERN_STRING + "'!");
        }

        return value;
    }

    public String getSource() {
        return source;
    }

    public String getUseCase() {
        return useCase;
    }

    public Optional<String> getConfiguration() {
        return Optional.ofNullable(configuration);
    }

    public DataFormat getDataFormat() {
        return dataFormat;
    }

    public DataIdentifier withConfiguration(String configuration) {
        return new DataIdentifier(source, useCase, configuration, dataFormat);
    }

    public DataIdentifier withDataFormat(DataFormat dataFormat) {
        return new DataIdentifier(source, useCase, configuration, dataFormat);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataIdentifier that = (DataIdentifier) o;
        return source.equals(that.source) &&
                useCase.equals(that.useCase) &&
                Objects.equals(configuration, that.configuration) &&
                dataFormat == that.dataFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, useCase, configuration, dataFormat);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(source).append(SEPARATOR)
                .append(useCase).append(SEPARATOR);

        if (configuration != null) {
            builder.append(configuration).append(SEPARATOR);
        }
        builder.append(dataFormat);
        return builder.toString();
    }

    public static DataIdentifier fromString(String input) {
        Objects.requireNonNull(input, "input must not be null!");
        StringTokenizer tok = new StringTokenizer(input, SEPARATOR);
        int tokenCount = tok.countTokens();
        if (tokenCount != 3 && tokenCount != 4) {
            throw new IllegalArgumentException("Expected three or four tokens separated by '" + SEPARATOR + "' but got " + tokenCount + "!");
        }
        String source = tok.nextToken();
        String useCase = tok.nextToken();
        String configuration = tokenCount == 4 ? tok.nextToken() : null;
        DataFormat dataFormat = DataFormat.valueOf(tok.nextToken());
        return new DataIdentifier(source, useCase, configuration, dataFormat);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(DataIdentifier other) {
        Objects.requireNonNull(other, "other must not be null!");

        int result = source.compareTo(other.source);
        if (result != 0) {
            return result;
        }

        result = useCase.compareTo(other.useCase);
        if (result != 0) {
            return result;
        }

        if (configuration == null) {
            if (other.configuration != null) {
                // identifier without configuration is always
                // "less" than one with a configuration
                return -1;
            }
        } else {
            if (other.configuration == null) {
                // identifier without configuration is always
                // "less" than one with a configuration
                return 1;
            }
            result = configuration.compareTo(other.configuration);

            if (result != 0) {
                return result;
            }
        }

        return dataFormat.compareTo(other.dataFormat);
    }
}
