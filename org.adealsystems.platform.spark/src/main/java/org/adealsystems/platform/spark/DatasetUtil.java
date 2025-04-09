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

package org.adealsystems.platform.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class DatasetUtil {

    private static final String TABLE_LINE_SEPARATOR_PREFIX = "+--";

    private DatasetUtil() {
    }

    public static Dataset<Row> createDataset(SparkSession spark, Map<String, Class<?>> types, String input) {
        String[] lines = input.split("\\n");
        List<Object[]> rows = new ArrayList<>();
        for (String line : lines) {
            if (line.startsWith(TABLE_LINE_SEPARATOR_PREFIX)) {
                continue;
            }
            rows.add(row(line));
        }

        Object[] header = rows.get(0);
        StructField[] fields = new StructField[header.length];
        for (int i = 0; i < header.length; i++) {
            // first row (header) has all values of type String
            String colName = String.valueOf(header[i]);
            Class<?> colType = types.get(colName);
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
                default:
                    throw new IllegalArgumentException("Unsupported type '" + colType + "'!");
            }
            fields[i] = new StructField(colName.trim(), dataType, true, Metadata.empty());
        }

        rows.remove(0);

        List<Row> dfRows = new ArrayList<>();
        for (Object[] row : rows) {
            dfRows.add(RowFactory.create(row));
        }

        return spark.createDataFrame(dfRows, new StructType(fields));
    }

    public static void assertDatasetEqual(String expected, Dataset<Row> dataset) {
        String[] lines = expected.split("\\n");
        boolean withHeaderRow = lines.length > 3 && lines[2].startsWith(TABLE_LINE_SEPARATOR_PREFIX);
        List<Object[]> rows = new ArrayList<>();
        for (String line : lines) {
            if (line.startsWith(TABLE_LINE_SEPARATOR_PREFIX)) {
                continue;
            }
            rows.add(row(line));
        }

        assertDatasetEqual(rows, withHeaderRow, dataset);
    }

    public static void assertDatasetEqual(List<Object[]> expected, boolean withHeaderRow, Dataset<Row> dataset) {
        int expectedRowsCount = withHeaderRow ? expected.size() - 1 : expected.size();
        long rowsCount = dataset.count();
        if (expectedRowsCount != rowsCount) {
            throw new AssertionError(String.format(
                Locale.ROOT,
                "Expected %d rows but got %d",
                expectedRowsCount, rowsCount
            ));
        }

        int r = 0;
        Iterator<Row> rowIterator = dataset.toLocalIterator();
        List<String> errors = new ArrayList<>();
        while (rowIterator.hasNext()) {
            if (!withHeaderRow || r > 0) {
                Object[] expectedRow = expected.get(r);
                Row row = rowIterator.next();
                if (expectedRow.length != row.size()) {
                    errors.add(String.format(
                        Locale.ROOT,
                        "Number of columns at row %d is wrong: expected %d, but found %d",
                        r, expectedRow.length, row.size()
                    ));
                } else {
                    for (int c = 0; c < row.size(); c++) {
                        if (isDifferent(expectedRow[c], row.get(c))) {
                            Object value = expectedRow[c];
                            String valueClass = value == null ? "<null>" : value.getClass().getName();
                            Object found = row.get(c);
                            String foundClass = found == null ? "unknown class" : found.getClass().getName();
                            String colHeader = withHeaderRow ? expected.get(0)[c] + " (" + c + ")" : String.valueOf(c);
                            errors.add(String.format(
                                Locale.ROOT,
                                "Value in row %d and column %s is wrong: expected '%s' (%s), but found '%s' (%s)",
                                r, colHeader, value, valueClass, found, foundClass
                            ));
                        }
                    }
                }
            }
            r++;
        }
        if (!errors.isEmpty()) {
            StringBuilder builder = new StringBuilder();
            for (String error : errors) {
                builder.append('\t').append(error).append('\n');
            }
            throw new AssertionError(String.format(Locale.ROOT, "\n%s", builder));
        }
    }

    public static Object[] row(String pipeSeparatedValues) {
        if (pipeSeparatedValues == null) {
            return Collections.emptyList().toArray(new Object[0]);
        }

        List<Object> values = new ArrayList<>();
        if (pipeSeparatedValues.startsWith("|")) {
            pipeSeparatedValues = pipeSeparatedValues.substring(1);
        }
        String[] tokens = pipeSeparatedValues.split("\\|");
        for (String token : tokens) {
            String value = token.trim();
            if ("null".equals(value)) {
                value = null;
            }
            values.add(parseObject(value));
        }

        return values.toArray(new Object[0]);
    }

    private static Object parseObject(String value) {
        if (value == null) {
            return null;
        }

        if (value.isEmpty()) {
            return "";
        }

        if (value.startsWith("\"") && value.endsWith("\"")) {
            // special case, String value inside of quotes
            return value.substring(1, value.length() - 1);
        }

        boolean maybeInt = true;
        boolean maybeLong = true;
        boolean maybeFloat = true;
        boolean maybeDouble = true;

        String upperCaseValue = value.toUpperCase(Locale.ROOT);
        String numberValue;

        if (upperCaseValue.endsWith("L")) {
            maybeInt = false;
            maybeDouble = false;
            maybeFloat = false;
            numberValue = value.substring(0, value.length() - 1);
        } else if (upperCaseValue.endsWith("D")) {
            maybeInt = false;
            maybeLong = false;
            maybeFloat = false;
            numberValue = value.substring(0, value.length() - 1);
        } else if (upperCaseValue.endsWith("F")) {
            maybeInt = false;
            maybeLong = false;
            maybeDouble = false;
            numberValue = value.substring(0, value.length() - 1);
        } else {
            numberValue = value;
        }

        if (maybeInt) {
            try {
                return Integer.parseInt(numberValue);
            } catch (NumberFormatException ex) {
                // ignore
            }
        }
        if (maybeLong) {
            try {
                return Long.parseLong(numberValue);
            } catch (NumberFormatException ex) {
                // ignore
            }
        }
        if (maybeFloat) {
            try {
                return Float.parseFloat(numberValue);
            } catch (NumberFormatException ex) {
                // ignore
            }
        }
        if (maybeDouble) {
            try {
                return Double.parseDouble(numberValue);
            } catch (NumberFormatException ex) {
                // ignore
            }
        }

        boolean maybeBoolean = "true".equals(value) || "false".equals(value);
        if (maybeBoolean) {
            return Boolean.parseBoolean(value);
        }

        return value;
    }

    private static boolean isDifferent(Object o1, Object o2) {
        if (o1 == null && o2 == null) {
            return false;
        }
        if (o1 == null || o2 == null) {
            return true;
        }
        return !o1.equals(o2);
    }
}
