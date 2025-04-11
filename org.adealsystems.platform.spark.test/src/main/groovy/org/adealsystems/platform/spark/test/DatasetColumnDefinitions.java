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

package org.adealsystems.platform.spark.test;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DatasetColumnDefinitions {
    private static final boolean DEFAULT_NULLABLE_COLUMN = true;
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    public static final String DEFAULT_DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm";

    private final List<String> header;
    private final Map<String, Class<?>> types;
    private final Map<String, Boolean> nullableFlags;
    private final Map<String, String> patterns;

    private DatasetColumnDefinitions(
        List<String> header,
        Map<String, Class<?>> types,
        Map<String, String> patterns,
        Map<String, Boolean> nullableFlags
    ) {
        this.types = Objects.requireNonNull(types, "types must not be null!");
        this.nullableFlags = Objects.requireNonNull(nullableFlags, "nullableFlags must not be null!");
        this.header = Objects.requireNonNull(header, "header must not be null!");
        this.patterns = Objects.requireNonNull(patterns, "patterns must not be null!");
    }

    public static DatasetColumnDefinitions.Builder builder() {
        return new DatasetColumnDefinitions.Builder();
    }

    public String getColumnName(int index) {
        return header.get(index);
    }

    public int getColumnCount() {
        return header.size();
    }

    public Class<?> getType(String columnName) {
        return types.get(columnName);
    }

    public Class<?> getType(int columnIndex) {
        String columnName = header.get(columnIndex);
        return types.get(columnName);
    }

    public boolean isNullable(String columnName) {
        return nullableFlags.get(columnName);
    }

    public boolean isNullable(int columnIndex) {
        String columnName = header.get(columnIndex);
        return nullableFlags.get(columnName);
    }

    public String getPattern(String columnName) {
        return patterns.get(columnName);
    }

    public String getPattern(int columnIndex) {
        String columnName = header.get(columnIndex);
        return patterns.get(columnName);
    }

    public StructField[] resolveDatasetStructure() {
        StructField[] fields = new StructField[header.size()];
        for (int i = 0; i < header.size(); i++) {
            // first row (header) has all values of type String
            String colName = String.valueOf(header.get(i));
            Class<?> colType = getType(colName);
            if (colType == null) {
                throw new IllegalArgumentException("Invalid column name! Missing type for column '" + colName + "'!");
            }

            DataType dataType;
            switch (colType.getName()) {
                case "java.lang.Integer":
                    dataType = DataTypes.IntegerType;
                    break;
                case "java.lang.Long":
                    dataType = DataTypes.LongType;
                    break;
                case "java.lang.Float":
                    dataType = DataTypes.FloatType;
                    break;
                case "java.lang.Double":
                    dataType = DataTypes.DoubleType;
                    break;
                case "java.lang.Boolean":
                    dataType = DataTypes.BooleanType;
                    break;
                case "java.lang.String":
                    dataType = DataTypes.StringType;
                    break;
                case "java.time.LocalDate":
                    dataType = DataTypes.DateType;
                    break;
                case "java.time.LocalDateTime":
                    dataType = DataTypes.TimestampType;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type '" + colType + "'!");
            }
            fields[i] = new StructField(
                colName.trim(),
                dataType,
                isNullable(colName),
                Metadata.empty()
            );
        }

        return fields;
    }

    public static class Builder {
        private final List<String> header = new ArrayList<>();
        private final Map<String, Class<?>> types = new HashMap<>();
        private final Map<String, Boolean> nullableFlags = new HashMap<>();
        private final Map<String, String> patterns = new HashMap<>();

        public Builder string(String columnName) {
            return column(columnName, String.class, null, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder string(String columnName, boolean nullable) {
            return column(columnName, String.class, null, nullable);
        }

        public Builder integer(String columnName) {
            return column(columnName, Integer.class, null, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder integer(String columnName, boolean nullable) {
            return column(columnName, Integer.class, null, nullable);
        }

        public Builder longint(String columnName) {
            return column(columnName, Long.class, null, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder longint(String columnName, boolean nullable) {
            return column(columnName, Long.class, null, nullable);
        }

        public Builder bool(String columnName) {
            return column(columnName, Boolean.class, null, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder bool(String columnName, boolean nullable) {
            return column(columnName, Boolean.class, null, nullable);
        }

        public Builder date(String columnName) {
            return column(columnName, LocalDate.class, DEFAULT_DATE_PATTERN, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder date(String columnName, boolean nullable) {
            return column(columnName, LocalDate.class, DEFAULT_DATE_PATTERN, nullable);
        }

        public Builder date(String columnName, String pattern) {
            return column(columnName, LocalDate.class, pattern, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder date(String columnName, String pattern, boolean nullable) {
            return column(columnName, LocalDate.class, pattern, nullable);
        }

        public Builder datetime(String columnName) {
            return column(columnName, LocalDateTime.class, DEFAULT_DATE_TIME_PATTERN, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder datetime(String columnName, boolean nullable) {
            return column(columnName, LocalDateTime.class, DEFAULT_DATE_TIME_PATTERN, nullable);
        }

        public Builder datetime(String columnName, String pattern) {
            return column(columnName, LocalDateTime.class, pattern, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder datetime(String columnName, String pattern, boolean nullable) {
            return column(columnName, LocalDateTime.class, pattern, nullable);
        }

        public Builder column(String columnName, Class<?> type) {
            return column(columnName, type, null, DEFAULT_NULLABLE_COLUMN);
        }

        public Builder column(String columnName, Class<?> type, boolean nullable) {
            return column(columnName, type, null, nullable);
        }

        public Builder column(String columnName, Class<?> type, String pattern, boolean nullable) {
            Objects.requireNonNull(columnName, "columnName must not be null!");
            Objects.requireNonNull(type, "type must not be null!");

            header.add(columnName);
            types.put(columnName, type);
            nullableFlags.put(columnName, nullable);
            patterns.put(columnName, pattern);

            return this;
        }

        public DatasetColumnDefinitions build() {
            return new DatasetColumnDefinitions(
                header,
                types,
                patterns,
                nullableFlags
            );
        }
    }
}
