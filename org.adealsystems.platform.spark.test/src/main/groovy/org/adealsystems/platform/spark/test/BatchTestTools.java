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

import org.adealsystems.platform.id.DataInstance;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.adealsystems.platform.id.DataInstances.createBatchReader;
import static org.adealsystems.platform.spark.test.DatasetColumnDefinitions.DEFAULT_DATE_PATTERN;
import static org.adealsystems.platform.spark.test.DatasetColumnDefinitions.DEFAULT_DATE_TIME_PATTERN;

public final class BatchTestTools {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchTestTools.class);

    public static final LocalDate TODAY = LocalDate.now(Clock.systemDefaultZone());

    private static final String TABLE_LINE_SEPARATOR_PREFIX = "+--";

    private static final Pattern DATE_PATTERN
        = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    private static final DateTimeFormatter DATE_FORMATTER
        = DateTimeFormatter.ofPattern(DEFAULT_DATE_PATTERN);

    private static final Pattern DATE_TIME_PATTERN
        = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}(:[0-9]{2})?");
    private static final DateTimeFormatter DATE_TIME_FORMATTER
        = DateTimeFormatter.ofPattern(DEFAULT_DATE_TIME_PATTERN);

    private static final Pattern QUERY_PATTERN
        = Pattern.compile("(?<target>[[a-zA-Z0-9_]+])(\\[(?<expr>.*)\\])?");
    private static final String EQUALITY_FORMAT = "([a-zA-Z0-9_]+)\\s*=\\s*([^\\s]+)";

    private BatchTestTools() {
    }

    public static String[] readBatchLines(DataInstance dataInstance) throws IOException {
        try (BufferedReader reader = createBatchReader(dataInstance)) {
            return reader.lines().collect(Collectors.toList()).toArray(new String[]{});
        }
    }

    // region reading dataset

    public static Dataset<Row> createDataset(SparkSession spark, DatasetColumnDefinitions colDefs, String input) {
        StructField[] fields = colDefs.resolveDatasetStructure();

        List<Row> dfRows = new ArrayList<>();
        RowIterator rowIterator = new RowIterator("\\|", input.split("\\n"));
        while (rowIterator.hasNext()) {
            String[] dataRow = rowIterator.next();
            dfRows.add(RowFactory.create(row(dataRow, colDefs)));
        }

        return spark.createDataFrame(dfRows, new StructType(fields));
    }

    public static Dataset<Row> createDataset(
        SparkSession spark,
        DatasetColumnDefinitions colDefs,
        RowFormat format,
        String... input
    ) {
        Objects.requireNonNull(spark, "spark must not be null!");
        Objects.requireNonNull(colDefs, "colDefs must not be null!");
        Objects.requireNonNull(format, "format must not be null!");
        Objects.requireNonNull(input, "input must not be null!");

        StructField[] fields = colDefs.resolveDatasetStructure();

        List<Row> dfRows = new ArrayList<>();
        RowIterator rowIterator = new RowIterator(format.getDelimiter(), input);
        while (rowIterator.hasNext()) {
            String[] dataRow = rowIterator.next();
            dfRows.add(RowFactory.create(row(dataRow, colDefs)));
        }

        return spark.createDataFrame(dfRows, new StructType(fields));
    }

    public static Dataset<Row> createDataset(SparkSession spark, DatasetColumnDefinitions colDefs, Rows rows) {
        StructField[] fields = colDefs.resolveDatasetStructure();

        List<Row> dfRows = new ArrayList<>();
        for (String[] dataRow : rows.rows) {
            dfRows.add(RowFactory.create(row(dataRow, colDefs)));
        }

        return spark.createDataFrame(dfRows, new StructType(fields));

    }

    public static class Rows {
        private final List<String[]> rows;

        public Rows(List<String[]> rows) {
            this.rows = rows;
        }

        public List<String[]> getRows() {
            return rows;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private final List<String[]> rows = new ArrayList<>();

            public Builder row(Object... cells) {
                List<String> converted = new ArrayList<>();
                for (Object cell : cells) {
                    converted.add(String.valueOf(cell));
                }
                rows.add(converted.toArray(new String[0]));
                return this;
            }

            public Rows build() {
                return new Rows(rows);
            }
        }
    }

    public static class RowFormat {
        private final String DEFAULT_DELIMITER = ",";

        private String delimiter = DEFAULT_DELIMITER;

        private RowFormat() {
        }

        public static RowFormat create() {
            return new RowFormat();
        }

        public RowFormat withDelimiter(String delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public String getDelimiter() {
            return delimiter;
        }
    }

    // endregion

    // region datasets equality check

    public static void assertDatasetEqual(Dataset<Row> expected, Dataset<Row> dataset) {
        long expectedRowsCount = expected.count();
        long rowsCount = dataset.count();
        if (expectedRowsCount != rowsCount) {
            throw new AssertionError(String.format(
                Locale.ROOT,
                "Expected %d rows, but got %d",
                expectedRowsCount, rowsCount
            ));
        }

        int r = 0;
        Iterator<Row> expIterator = expected.toLocalIterator();
        Iterator<Row> rowIterator = dataset.toLocalIterator();
        List<String> errors = new ArrayList<>();
        while (rowIterator.hasNext()) {
            Row exp = expIterator.next();
            Row row = rowIterator.next();

            if (exp.size() != row.size()) {
                errors.add(String.format(
                    Locale.ROOT,
                    "Number of columns at row %d is wrong: expected %d, but found %d",
                    r, exp.size(), row.size()
                ));
            }
            else {
                for (int c = 0; c < row.size(); c++) {
                    if (isDifferent(exp.get(c), row.get(c))) {
                        Object value = exp.get(c);
                        String valueClass = value == null ? "<null>" : value.getClass().getName();
                        Object found = row.get(c);
                        String foundClass = found == null ? "unknown class" : found.getClass().getName();
                        String colHeader = String.valueOf(c);
                        errors.add(String.format(
                            Locale.ROOT,
                            "Value in row %d and column %s is wrong: expected '%s' (%s), but found '%s' (%s)",
                            r, colHeader, value, valueClass, found, foundClass
                        ));
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

    // endregion

    // region value selection

    public static <T> Collection<T> valuesAt(Dataset<Row> dataset, String query, Class<T> targetClass) {
        Matcher matcher = QUERY_PATTERN.matcher(query);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid query format: '" + query + "'!");
        }

        String target = matcher.group("target");
        String expr = matcher.group("expr");

        Optional<String> parsedFilter = parseExpression(expr);
        if (parsedFilter.isPresent()) {
            dataset = dataset.filter(parsedFilter.get());
        }
        Dataset<Row> selected = dataset.select(target);

        return selected.collectAsList()
            .stream()
            .map(row -> targetClass.cast(row.getAs(target)))
            .collect(Collectors.toSet());
    }

    private static Optional<String> parseExpression(String expr) {
        if (expr == null || expr.isEmpty()) {
            return Optional.empty();
        }

        String parsed = expr
            .replace("&", "AND")
            .replace("|", "OR");

        // Ensure proper formatting for equality checks
        parsed = parsed.replaceAll(EQUALITY_FORMAT, "$1 = $2");

        return Optional.of(parsed);
    }

    // endregion

    // region helpers

    private static String[] parseRow(String row, String valueDelimiter) {
        if (row == null) {
            return new String[0];
        }

        List<String> values = new ArrayList<>();
        if (row.startsWith("|")) {
            row = row.substring(1);
        }
        String[] tokens = row.split(valueDelimiter);
        for (String token : tokens) {
            values.add(token.trim());
        }

        return values.toArray(new String[0]);
    }

    private static Object[] row(String[] cells, DatasetColumnDefinitions colDefs) {
        if (cells == null) {
            return new Object[0];
        }

        if (colDefs != null) {
            if (colDefs.getColumnCount() != cells.length) {
                throw new IllegalArgumentException("Number of type definitions does not match number of cells!");
            }
        }

        List<Object> values = new ArrayList<>();
        for (int i = 0; i < cells.length; i++) {
            String cell = cells[i];
            Class<?> type = null;
            boolean nullable = true;
            String pattern = null;
            if (colDefs != null) {
                type = colDefs.getType(i);
                nullable = colDefs.isNullable(i);
                pattern = colDefs.getPattern(i);
            }

            String value = cell.trim();
            if ("null".equals(value)) {
                value = null;
            }
            Object obj = parseObject(value, type, pattern);
            if (obj == null && !nullable) {
                String colName = colDefs.getColumnName(i);
                String msg = String.format(Locale.ROOT, "Value is null for a non nullable column '%s'!", colName);
                throw new IllegalArgumentException(msg);
            }
            values.add(obj);
        }

        return values.toArray(new Object[0]);
    }

    private static Object parseObject(String value, Class<?> type, String pattern) {
        if (value == null) {
            return null;
        }

        if (value.isEmpty()) {
            if (type == null) {
                return "";
            }
            if (String.class.equals(type)) {
                return "";
            }
            return null;
        }

        if (value.startsWith("\"") && value.endsWith("\"")) {
            // special case, String value inside of quotes
            return value.substring(1, value.length() - 1);
        }

        return type != null
            ? parseTypedObject(value, type, pattern)
            : parseUntypedObject(value);
    }

    private static Object parseTypedObject(String value, Class<?> type, String pattern) {
        String upperCaseValue = value.toUpperCase(Locale.ROOT);

        switch (type.getName()) {
            case "java.lang.String":
                return value;

            case "java.lang.Integer":
                try {
                    return Integer.parseInt(value);
                }
                catch (NumberFormatException ex) {
                    LOGGER.warn("Could not parse integer from value '{}'!", value);
                    return null;
                }

            case "java.lang.Long":
                try {
                    if (upperCaseValue.endsWith("L")) {
                        value = value.substring(0, value.length() - 1);
                    }
                    return Long.parseLong(value);
                }
                catch (NumberFormatException ex) {
                    LOGGER.warn("Could not parse long from value '{}'!", value);
                    return null;
                }

            case "java.lang.Float":
                try {
                    return Float.parseFloat(value);
                }
                catch (NumberFormatException ex) {
                    LOGGER.warn("Could not parse float from value '{}'!", value);
                    return null;
                }

            case "java.lang.Double":
                try {
                    if (upperCaseValue.endsWith("D")) {
                        value = value.substring(0, value.length() - 1);
                    }
                    return Double.parseDouble(value);
                }
                catch (NumberFormatException ex) {
                    LOGGER.warn("Could not parse double from value '{}'!", value);
                    return null;
                }

            case "java.lang.Boolean":
                switch (value) {
                    case "true":
                        return true;
                    case "false":
                        return false;
                    default:
                        LOGGER.warn("Could not parse boolean from value '{}'!", value);
                        return null;
                }

            case "java.time.LocalDate":
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                        pattern == null ? DEFAULT_DATE_PATTERN : pattern
                    );
                    return LocalDate.parse(value, formatter);
                }
                catch (DateTimeParseException ex) {
                    LOGGER.warn("Could not parse date from value '{}'!", value);
                    return null;
                }

            case "java.time.LocalDateTime":
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(
                        pattern == null ? DEFAULT_DATE_TIME_PATTERN : pattern
                    );
                    return Timestamp.valueOf(LocalDateTime.parse(value, formatter));
                }
                catch (DateTimeParseException ex) {
                    LOGGER.warn("Could not parse datetime from value '{}'!", value);
                    return null;
                }

            default:
                throw new IllegalArgumentException(
                    String.format("Type '%s' is not supported for value '%s'!", type.getName(), value)
                );
        }
    }

    private static Object parseUntypedObject(String value) {
        if ("true".equals(value) || "false".equals(value)) {
            return Boolean.parseBoolean(value);
        }

        if (DATE_PATTERN.matcher(value).matches()) {
            return LocalDate.parse(value, DATE_FORMATTER);
        }
        if (DATE_TIME_PATTERN.matcher(value).matches()) {
            return Timestamp.valueOf(LocalDateTime.parse(value, DATE_TIME_FORMATTER));
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
        }
        else if (upperCaseValue.endsWith("D")) {
            maybeInt = false;
            maybeLong = false;
            maybeFloat = false;
            numberValue = value.substring(0, value.length() - 1);
        }
        else if (upperCaseValue.endsWith("F")) {
            maybeInt = false;
            maybeLong = false;
            maybeDouble = false;
            numberValue = value.substring(0, value.length() - 1);
        }
        else {
            numberValue = value;
        }

        if (maybeInt) {
            try {
                return Integer.parseInt(numberValue);
            }
            catch (NumberFormatException ex) {
                // ignore
            }
        }
        if (maybeLong) {
            try {
                return Long.parseLong(numberValue);
            }
            catch (NumberFormatException ex) {
                // ignore
            }
        }
        if (maybeFloat) {
            try {
                return Float.parseFloat(numberValue);
            }
            catch (NumberFormatException ex) {
                // ignore
            }
        }
        if (maybeDouble) {
            try {
                return Double.parseDouble(numberValue);
            }
            catch (NumberFormatException ex) {
                // ignore
            }
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

    private static class RowIterator implements Iterator<String[]> {
        private final Queue<String> queue = new LinkedList<>();
        private final String valueDelimiter;

        public RowIterator(String valueDelimiter, String[] lines) {
            this.valueDelimiter = Objects.requireNonNull(valueDelimiter, "valueDelimiter must not be null!");
            Objects.requireNonNull(lines, "lines must not be null!");

            boolean skipHeader;
            if (lines.length >= 3) {
                // header is only available if more than 3 lines specified & 1st and 3rd are borderlines
                skipHeader = lines[0].startsWith(TABLE_LINE_SEPARATOR_PREFIX)
                    && lines[2].startsWith(TABLE_LINE_SEPARATOR_PREFIX);
            }
            else {
                skipHeader = false;
            }

            for (String line : lines) {
                if (line.startsWith(TABLE_LINE_SEPARATOR_PREFIX)) {
                    // ignore all borderlines (if available)
                    continue;
                }
                if (skipHeader) {
                    // skip header line
                    skipHeader = false;
                    continue;
                }

                // add a data line to the processing queue
                queue.add(line);
            }
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public String[] next() {
            return parseRow(queue.poll(), valueDelimiter);
        }
    }

    // endregion
}
