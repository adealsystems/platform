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

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * This utility class can be used to log dataset states and should help
 * on analysing of complex tasks.
 *
 * <h2>Examples</h2>
 *
 *
 *
 * <h3>Show multiple datasets</h3>
 *
 * <pre>
 * DatasetLogger DSLOG = new DatasetLogger();
 *
 * Dataset&lt;Row&gt; dsA = ...
 * DSLOG.showInfo("Headline message A", dsA);
 *
 * Dataset&lt;Row&gt; dsB = ...
 * DSLOG.showInfo("Headline message B", dsB);
 * </pre>
 * <p>
 * produces the following output:
 *
 * <pre>
 * Headline message A:
 * source: com.foo.bar.YourClass (YourClass.java:98)
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * |456     |BBB          |YYYYY   |
 * +--------+-------------+--------+
 *
 * Headline message B:
 * source: com.foo.bar.YourClass (YourClass.java:101)
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * |456     |BBB          |YYYYY   |
 * +--------+-------------+--------+
 * </pre>
 * <p>
 * As you can see, different headline messages separate the output and describe
 * dataset outputs. In addition, if you run your application inside your IDE,
 * you can click the class name to jump to the row in the source code.
 *
 * <h3>Show dataset with on-the-fly filtering by column</h3>
 * <p>
 * You can specify as many column filters as needed and pass all of them comma
 * separated to the <code>showInfo()</code> method.
 * <pre>
 * DatasetLogger DSLOG = new DatasetLogger();
 *
 * Dataset&lt;Row&gt; dsA = ...
 * DSLOG.showInfo("Headline message A", dsA, col("COL-1").equalsTo(123));
 * </pre>
 * <p>
 * produces the following output:
 *
 * <pre>
 * Headline message A:
 * source: com.foo.bar.YourClass (YourClass.java:98)
 * context: (COL-1 = 123)
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * +--------+-------------+--------+
 * </pre>
 *
 *
 *
 * <h3>Show dataset with context-specific filter settings</h3>
 * <p>
 * You can specify as many column filters as needed and pass all of them comma
 * separated to the <code>Context.setColumns()</code> method.
 * <pre>
 * DatasetLogger DSLOG = new DatasetLogger();
 * DatasetLogger.Context ctx = DatasetLogger.newContext();
 * ctx.setColumns(col("COL-1").equalsTo(123), [...]);
 *
 * Dataset&lt;Row&gt; dsA = ...
 * DSLOG.showInfo(ctx, "Headline message A", dsA);
 *
 * Dataset&lt;Row&gt; dsB = ...
 * DSLOG.showInfo(ctx, "Headline message B", dsB);
 * </pre>
 * <p>
 * produces the following output:
 *
 * <pre>
 * Headline message A:
 * source: com.foo.bar.YourClass (YourClass.java:98)
 * context: (COL-1 = 123) ...
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * +--------+-------------+--------+
 *
 * Headline message B:
 * source: com.foo.bar.YourClass (YourClass.java:101)
 * context: (COL-1 = 123) ...
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * +--------+-------------+--------+
 * </pre>
 * <p>
 * Here you are passing a filter context (with one or more column filters)
 * to all <code>showInfo()</code> calls. This feature is very useful if you are analysing
 * the batch and focusing on a specific data values. In this way you can
 * specify your search criteria ones and pass them to all your <code>showInfo()</code>
 * calls. If your requirements change you only modify the context on top of your code.
 *
 *
 *
 * <h3>Stash and unstash of context</h3>
 * <p>
 * Sometimes you have one temporary dataset with a different structure. In this case you
 * can 'stash' your context, apply other filter rules to that different dataset, and
 * 'unstash' your original context afterwards to use it for further calls. Or you can
 * just use different contexts for different calls.
 *
 * <pre>
 * DatasetLogger DSLOG = new DatasetLogger();
 * DatasetLogger.Context ctx = DatasetLogger.newContext();
 * ctx.setColumns(col("COL-1").equalsTo(123), [...]);
 *
 * Dataset&lt;Row&gt; dsA = ...
 * DSLOG.showInfo(ctx, "Headline message A", dsA);
 *
 * ctx.stashAndClear(); // stashes the context
 *
 * // here the context is empty
 *
 * Dataset&lt;Row&gt; dsB = ...
 * DSLOG.showInfo(ctx, "Headline message B (without context)", dsB);
 *
 * ctx.unstash(); // restore stashed context
 *
 * // the context here contains the original filter
 *
 * DSLOG.showInfo(ctx, "Headline message B (with context)", dsB);
 * </pre>
 * <p>
 * produces the following output:
 *
 * <pre>
 * Headline message A:
 * source: com.foo.bar.YourClass (YourClass.java:98)
 * context: (COL-1 = 123) ...
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * +--------+-------------+--------+
 *
 * Headline message B (without context):
 * source: com.foo.bar.YourClass (YourClass.java:101)
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * |456     |BBB          |YYYYY   |
 * +--------+-------------+--------+
 *
 * Headline message B (with context):
 * source: com.foo.bar.YourClass (YourClass.java:101)
 * context: (COL-1 = 123) ...
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * +--------+-------------+--------+
 * </pre>
 *
 *
 *
 * <h3>Enabling and disabling</h3>
 * <p>
 * In some cases you are debugging your batch and would like to concentrate on a
 * concrete step. You can disable the output of DatasetLogger and re-enable it
 * before executing your step. So you can avoid output of all steps before.
 *
 * <pre>
 * DatasetLogger DSLOG = new DatasetLogger();
 * DatasetLogger.Context ctx = DatasetLogger.newContext();
 *
 * ctx.disable(); // disabling the output, don't need to see A.1, A.2
 *
 * Dataset&lt;Row&gt; dsA = ...
 * DSLOG.showInfo(ctx, "Headline message A.1", dsA);
 *
 * DSLOG.showInfo(ctx, "Headline message A.2", dsA);
 *
 * ctx.enable(); // enabling the output, would like to see A.3
 *
 * DSLOG.showInfo(ctx, "Headline message A.3", dsA);
 * </pre>
 * <p>
 * produces the following output:
 *
 * <pre>
 * Headline message A.3:
 * source: com.foo.bar.YourClass (YourClass.java:98)
 * +--------+-------------+--------+
 * |COL-1   |COL-2        |COL-3   |
 * +--------+-------------+--------+
 * |123     |AAA          |XXXXX   |
 * |456     |BBB          |YYYYY   |
 * +--------+-------------+--------+
 * </pre>
 *
 *
 *
 * <h3>Columns of a single dataset</h3>
 * <p>
 * Sometimes you just need to see columns of a dataset.
 * <pre>
 * DatasetLogger DSLOG = new DatasetLogger();
 *
 * Dataset&lt;Row&gt; dsA = ...
 * DSLOG.showColumns("Columns of a dataset A", dsA);
 * </pre>
 * <p>
 * produces the following output:
 *
 * <pre>
 * Columns of a dataset A:
 * [COL-1, COL-2, COL-3]
 * source: com.foo.bar.YourClass (YourClass.java:98)
 * </pre>
 * <p>
 * You can pass more than one dataset. In this case you will see a compare table for columns
 * of all specified datasets.
 *
 * <pre>
 * DatasetLogger DSLOG = new DatasetLogger();
 *
 * Dataset&lt;Row&gt; dsA = ...
 * Dataset&lt;Row&gt; dsB = ...
 * DSLOG.showColumns("Columns of a dataset A and B", dsA, dsB);
 * </pre>
 * <p>
 * produces the following output:
 *
 * <pre>
 * Columns of a dataset A and B:
 *            1   2
 * -----------------
 * COL-1 .... +   -
 * COL-2 .... +   +
 * COL-2-B .. +   +
 * COL-3 .... +   -
 * COL-4-B .. -   +
 * COLUMN-10  -   +
 * source: com.foo.bar.YourClass (YourClass.java:98)
 * </pre>
 * <p>
 * In this compare table you can see columns existing in only the first dataset,
 * only the second dataset or in both datasets.
 * <p>
 * You can pass a context to the <code>showColumns()</code> call too. The only
 * available setting for columns is enable/disable flag.
 */
public class DatasetLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetLogger.class);

    private static boolean disabledGlobally = false;
    private static boolean firstCall = true;

    private final Context context;

    private final Map<String, AnalyserSession> analyserSessions = new HashMap<>();
    private final Set<String> activeSessions = new HashSet<>();
    private final Map<String, Consumer<AnalyserSession>> analysers = new HashMap<>();

    public DatasetLogger() {
        this(null);
    }

    public DatasetLogger(Context context) {
        if (context == null) {
            context = Context.getDefaultContext();
        }
        this.context = context;
    }

    public static boolean isDisabledGlobally() {
        return disabledGlobally;
    }

    public static void setDisabledGlobally(boolean disabledGlobally) {
        DatasetLogger.disabledGlobally = disabledGlobally;
        firstCall = true; // next call will be considered first call again
    }

    private static boolean isDisabled(Context ctx) {
        if (isDisabledGlobally()) {
            if (firstCall) {
                LOGGER.info("DatasetLogger output is disabled.");
                firstCall = false;
            }
            return true;
        }

        return !ctx.isEnabled();
    }

    // region show columns

    public void showColumns(String message, Dataset<?>... datasets) {
        showColumns(null, message, datasets);
    }

    public void showColumns(Context ctx, String message, Dataset<?>... datasets) {
        ctx = resolveContext(ctx);

        if (isDisabled(ctx)) {
            return;
        }

        Objects.requireNonNull(datasets, "datasets must not be null!");
        for (int i = 0; i < datasets.length; i++) {
            Objects.requireNonNull(datasets[i], "datasets[" + i + "] must not be null!");
        }

        StringBuilder builder = new StringBuilder();

        // message
        builder.append(message).append('\n');

        // source link
        String sourceLink = buildSourceOutput("showColumns");
        if (sourceLink != null) {
            builder.append(sourceLink).append('\n');
        }

        // dataset columns
        if (datasets.length == 1) {
            // only one dataset, show just the column list
            String[] columnNames = datasets[0].columns();
            builder.append('\t').append(Arrays.asList(columnNames)).append('\n');
        } else {
            // multiple datasets, show the compare table
            StringBuilder patternBuilder = new StringBuilder();

            int length = datasets.length;
            Object[] header = new String[length + 1];
            Object[] divider = new String[length + 1];

            header[0] = "";

            Set<String> allColumns = new HashSet<>();
            List<Set<String>> datasetColumns = new ArrayList<>();

            for (int i = 1; i <= datasets.length; i++) {
                Dataset<?> dataset = datasets[i - 1];
                Set<String> cols = new HashSet<>(Arrays.asList(dataset.columns())); // NOPMD - checked against AvoidInstantiatingObjectsInLoops
                datasetColumns.add(cols);
                allColumns.addAll(cols);
                patternBuilder.append("%3s");
                header[i] = " " + i + " ";
                divider[i] = "---";
            }

            int maxColumnNameLength = 0;
            for (String column : allColumns) {
                maxColumnNameLength = Math.max(maxColumnNameLength, column.length());
            }
            maxColumnNameLength++;

            patternBuilder.insert(0, "%-" + maxColumnNameLength + "s");
            divider[0] = fillString("", '-', maxColumnNameLength);

            List<String> sortedColumns = new ArrayList<>(allColumns);
            Collections.sort(sortedColumns);

            String pattern = patternBuilder.toString();
            builder.append(String.format(Locale.US, pattern, header)).append('\n');
            builder.append(String.format(Locale.US, pattern, divider)).append('\n');
            for (String column : sortedColumns) {
                Object[] line = new String[length + 1];  // NOPMD - checked against AvoidInstantiatingObjectsInLoops
                line[0] = fillString(column + ' ', '.', maxColumnNameLength);
                for (int i = 1; i <= length; i++) {
                    line[i] = datasetColumns.get(i - 1).contains(column) ? " + " : " - ";
                }
                builder.append(String.format(Locale.US, pattern, line)).append('\n');
            }
        }

        ctx.getLogger().info("\n{}", builder);
    }

    // endregion

    // region show schema

    public void showSchema(String message, Dataset<Row> dataset) {
        showSchema(null, message, dataset);
    }

    public void showSchema(Context ctx, String message, Dataset<Row> dataset) {
        ctx = resolveContext(ctx);

        if (isDisabled(ctx)) {
            return;
        }

        StringBuilder builder = new StringBuilder();

        // message
        builder.append(message).append(" [size: ").append(dataset.count()).append("]:\n");

        // source link
        String sourceLink = buildSourceOutput("showSchema");
        if (sourceLink != null) {
            builder.append(sourceLink).append('\n');
        }

        StructType schema = dataset.schema();
        String schemaTree = schema.treeString();
        builder.append(schemaTree);

        ctx.getLogger().info("\n{}", builder);
    }

    // endregion

    // region show struct

    public void showStruct(String message, StructType structType) {
        showStruct(null, message, structType);
    }

    public void showStruct(Context ctx, String message, StructType structType) {
        ctx = resolveContext(ctx);

        if (isDisabled(ctx)) {
            return;
        }

        StringBuilder builder = new StringBuilder();

        // message
        builder.append(message).append(" [size: ").append(structType == null ? "N/A" : structType.size()).append("]:\n");

        // source link
        String sourceLink = buildSourceOutput("showStruct");
        if (sourceLink != null) {
            builder.append(sourceLink).append('\n');
        }

        if (structType == null) {
            builder.append("StructType is null");
        } else {
            StructField[] fields = structType.fields();
            int maxNameLength = 0;
            int maxTypeLength = 0;
            for (StructField field : fields) {
                maxNameLength = Math.max(maxNameLength, field.name().length());
                maxTypeLength = Math.max(maxTypeLength, field.dataType().typeName().length());
            }

            String divider = fillString("+", '-', maxNameLength + 3) + fillString("+", '-', maxTypeLength + 3) + "+\n";
            builder.append(divider);
            for (StructField field : fields) {
                String name = fillString(field.name(), ' ', maxNameLength);
                String type = fillString(field.dataType().typeName(), ' ', maxTypeLength);
                builder.append("| ").append(name).append(" | ").append(type).append(" |\n");
            }
            builder.append(divider);
        }

        ctx.getLogger().info("\n{}", builder);
    }

    // endregion

    // region show info

    public void showInfo(String message, Dataset<Row> dataset, Column... columns) {
        showInfo(null, message, dataset, columns);
    }

    public void showInfo(Context ctx, String message, Dataset<Row> dataset, Column... columns) {
        ctx = resolveContext(ctx);

        if (isDisabled(ctx)) {
            return;
        }

        StringBuilder builder = new StringBuilder();

        // message
        builder.append(message).append(" [size: ").append(dataset == null ? "N/A" : dataset.count()).append("]:\n");

        // source link
        String sourceLink = buildSourceOutput("showInfo");
        if (sourceLink != null) {
            builder.append(sourceLink).append('\n');
        }

        // columns (filters)
        List<Column> currentColumns = new ArrayList<>();
        List<Column> ctxColumns = ctx.getColumns();
        if (!ctxColumns.isEmpty()) {
            currentColumns.addAll(ctxColumns);
        }
        if (columns != null && columns.length > 0) {
            currentColumns.addAll(Arrays.asList(columns));
        }

        final int numberOfRows = ctx.getNumberOfRows();

        StringBuilder contextBuilder = new StringBuilder();
        if (!currentColumns.isEmpty()) {
            for (Column column : currentColumns) {
                if (dataset != null) {
                    dataset = dataset.where(column);
                }

                if (contextBuilder.length() > 0) {
                    contextBuilder.append(", ");
                }
                contextBuilder.append(column);
            }
        }
        if (numberOfRows != Context.DEFAULT_NUMBER_OF_ROWS) {
            if (contextBuilder.length() > 0) {
                contextBuilder.append(", ");
            }
            contextBuilder.append("numberOfRows = ").append(numberOfRows);
        }

        if (contextBuilder.length() > 0) {
            builder.append("\tcontext: ").append(contextBuilder).append('\n');
        }

        if (dataset == null) {
            builder.append("Dataset is null");
        } else {
            builder.append(dataset.showString(numberOfRows, 0, false));
        }

        ctx.getLogger().info("\n{}", builder);
    }

    // endregion

    // region analyser

    public void addSessionAnalyser(String sessionCode, Consumer<AnalyserSession> analyser) {
        Objects.requireNonNull(sessionCode, "session code must not be null!");
        Objects.requireNonNull(analyser, "analyser must not be null!");

        this.analysers.put(sessionCode, analyser);
    }

    public void startSession(String code) {
        Objects.requireNonNull(code, "session code must not be null!");

        LOGGER.debug("Starting analysis session {}", code);
        analyserSessions.put(code, new AnalyserSession(code));
        activeSessions.add(code);
    }

    public void stopSession(String code) {
        Objects.requireNonNull(code, "session code must not be null!");

        AnalyserSession session = analyserSessions.get(code);
        if (session == null) {
            throw new IllegalArgumentException("No analysis session found for '" + code + "'");
        }

        LOGGER.debug("Stopping analysis session {}", code);
        session.close();
        activeSessions.remove(code);
    }

    public void analyse(String analysisCode, Dataset<Row> dataset) {
        Objects.requireNonNull(analysisCode, "analysisCode code must not be null!");
        Objects.requireNonNull(dataset, "dataset must not be null!");

        if (activeSessions.isEmpty()) {
            LOGGER.debug("No active analysis sessions found, skipping dataset analysis");
            return;
        }

        for (String code : activeSessions) {
            AnalyserSession session = analyserSessions.get(code);
            if (session == null) {
                throw new IllegalArgumentException("No analysis session found for '" + code + "'");
            }

            Consumer<AnalyserSession> analyser = analysers.get(code);
            if (analyser == null) {
                LOGGER.debug("No analyser specified for '{}', skipping analysis '{}'", code, analysisCode);
                return;
            }

            LOGGER.debug("Adding dataset {} to active analysis session '{}'", analysisCode, code);
            session.add(analysisCode, dataset);

            LOGGER.debug("Calling analyser for session '{}' and analysis '{}'", code, analysisCode);
            analyser.accept(session);
        }
    }

    // endregion

    private DatasetLogger.Context resolveContext(DatasetLogger.Context ctx) {
        return ctx == null ? context : ctx;
    }

    private static String buildSourceOutput(String method) {
        StackTraceElement[] stackTrace = new RuntimeException().getStackTrace();
        int last = Integer.MIN_VALUE;
        for (int i = 0; i < stackTrace.length; i++) {
            StackTraceElement current = stackTrace[i];

            if (DatasetLogger.class.getName().equals(current.getClassName())
                && method.equals(current.getMethodName())) {
                last = i;
            }
        }
        // use the element after the last matching element,
        // i.e. the caller of "method"
        last++;

        if (last > 0 // there was an entry
            && last < stackTrace.length) {
            StackTraceElement causeElement = stackTrace[last];

            return "\tsource: "
                + causeElement.getClassName()
                + " (" + causeElement.getFileName()
                + ":" + causeElement.getLineNumber() + ")";
        }
        return null; // failed to obtain source for whatever reason
        // (This could actually happen. It happened before for NullPointerException.)
    }

    public static Context newContext() {
        return new Context();
    }

    public static class Context {
        public static final int DEFAULT_NUMBER_OF_ROWS = 20;

        private static final Context DEFAULT_CONTEXT = new Context();

        private Logger logger;
        private List<Column> columns = new ArrayList<>();

        private final List<List<Column>> stashes = new ArrayList<>();

        private boolean enabled = true;

        private int numberOfRows = DEFAULT_NUMBER_OF_ROWS;

        private Context() {
            // use DatasetLogger.newContext() factory method
        }

        @SuppressWarnings("PMD.UnusedPrivateMethod") // false positive PMD 6.33.0
        private static Context getDefaultContext() {
            return DEFAULT_CONTEXT;
        }

        public void setColumns(Column... context) {
            Objects.requireNonNull(context, "context must not be null!");
            columns.clear();
            columns.addAll(Arrays.asList(context));
        }

        public void addColumn(Column column) {
            columns.add(column);
        }

        public List<Column> getColumns() {
            return columns;
        }

        public int getNumberOfRows() {
            return numberOfRows;
        }

        public void setNumberOfRows(int numberOfRows) {
            if (numberOfRows <= 0) {
                throw new IllegalArgumentException("numberOfRows must be positive!");
            }
            this.numberOfRows = numberOfRows;
        }

        public void clear() {
            columns.clear();
            numberOfRows = DEFAULT_NUMBER_OF_ROWS;
        }

        public void stash() {
            if (columns.isEmpty()) {
                return;
            }

            push(stashes, new ArrayList<>(columns));
        }

        public void stashAndClear() {
            stash();
            clear();
        }

        public void unstash() {
            if (stashes.isEmpty()) {
                return;
            }

            columns = pop(stashes);
        }

        /**
         * @return the logger to be used for reporting.
         */
        public Logger getLogger() {
            return logger == null ? LOGGER : logger;
        }

        /**
         * Sets logger to be used for reporting.
         *
         * @param logger the logger to use. null uses default logger of DatasetLogger.
         */
        public void setLogger(Logger logger) {
            this.logger = logger;
        }

        public void enable() {
            enabled = true;
        }

        public void disable() {
            enabled = false;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isEnabled() {
            return enabled && getLogger().isInfoEnabled();
        }
    }

    public static class AnalyserSession {
        private final String code;
        private final Map<String, Dataset<Row>> datasets = new HashMap<>();

        private final LocalDateTime startTime;
        private LocalDateTime lastUpdateTime;
        private LocalDateTime stopTime;
        private boolean closed;

        public AnalyserSession(String code) {
            this.code = code;
            this.startTime = LocalDateTime.now(ZoneId.systemDefault());
            this.closed = false;
        }

        public void close() {
            this.stopTime = LocalDateTime.now(ZoneId.systemDefault());
            this.closed = true;
            for (Dataset<Row> dataset : datasets.values()) {
                dataset.unpersist();
            }
        }

        public void add(String key, Dataset<Row> dataset) {
            datasets.put(key, dataset);
            this.lastUpdateTime = LocalDateTime.now(ZoneId.systemDefault());
        }

        public String getCode() {
            return code;
        }

        public Map<String, Dataset<Row>> getDatasets() {
            return datasets;
        }

        public LocalDateTime getStartTime() {
            return startTime;
        }

        public LocalDateTime getLastUpdateTime() {
            return lastUpdateTime;
        }

        public LocalDateTime getStopTime() {
            return stopTime;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            AnalyserSession that = (AnalyserSession) o;
            return closed == that.closed
                && Objects.equals(code, that.code)
                && Objects.equals(datasets, that.datasets)
                && Objects.equals(startTime, that.startTime)
                && Objects.equals(lastUpdateTime, that.lastUpdateTime)
                && Objects.equals(stopTime, that.stopTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, datasets, startTime, lastUpdateTime, stopTime, closed);
        }

        @Override
        public String toString() {
            return "AnalyserSession{" +
                "code='" + code + '\'' +
                ", datasets=" + datasets +
                ", startTime=" + startTime +
                ", lastUpdateTime=" + lastUpdateTime +
                ", stopTime=" + stopTime +
                ", closed=" + closed +
                '}';
        }
    }

    private static String fillString(String value, char ch, int size) {
        StringBuilder builder = new StringBuilder(value);

        while (builder.length() < size) {
            builder.append(ch);
        }

        return builder.toString();
    }

    private static <T> void push(List<T> stack, T entry) {
        stack.add(entry);
    }

    private static <T> T pop(List<T> stack) {
        int len = stack.size();
        if (len == 0) {
            return null;
        }

        return stack.remove(len - 1);
    }
}
