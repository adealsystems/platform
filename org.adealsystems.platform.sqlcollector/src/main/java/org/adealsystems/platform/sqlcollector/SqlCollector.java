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

package org.adealsystems.platform.sqlcollector;

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.DrainFactory;
import org.adealsystems.platform.state.ProcessingState;
import org.adealsystems.platform.state.ProcessingStateFileFactory;
import org.adealsystems.platform.state.impl.ProcessingStateFileWriter;
import org.adealsystems.platform.time.DurationFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SqlCollector<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlCollector.class);

    private static final int MILLIS_IN_NANO = 1_000_000;
    private static final int WORKER_THREAD_COUNT = 4;
    private static final long SUPERVISOR_CHECK_INTERVAL = 60 * 1_000; // 60 seconds

    private final ThreadLocal<SqlClientBundle> clientBundleThreadLocal = new ThreadLocal<>();
    private final BlockingQueue<QueryEntity> incomingQueue;
    private final BlockingQueue<QueryEntity> failedQueue;
    private final BlockingQueue<Q> successQueue;
    private final QueryEntity sentinel = new QueryEntity();
    private final SqlClientFactory clientFactory;
    private final SqlQuery<Q, R> sqlQuery;
    private final DrainFactory<Q, R> drainFactory;
    private final Drain<SqlMetrics<Q>> metricsDrain;
    private final ProcessingStateFileFactory<Q> processingStateFileFactory;
    private final DataSource dataSource;
    private final ExecutorService supervisorExecutor;

    public SqlCollector(
        DataSource dataSource,
        SqlClientFactory clientFactory,
        SqlQuery<Q, R> sqlQuery,
        DrainFactory<Q, R> drainFactory,
        Drain<SqlMetrics<Q>> metricsDrain,
        ProcessingStateFileFactory<Q> processingStateFileFactory,
        int queueSize
    ) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null!");
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory must not be null!");
        this.sqlQuery = Objects.requireNonNull(sqlQuery, "sqlQuery must not be null!");
        this.drainFactory = Objects.requireNonNull(drainFactory, "drainFactory must not be null!");
        this.metricsDrain = metricsDrain;
        this.processingStateFileFactory = processingStateFileFactory;

        this.supervisorExecutor = Executors.newSingleThreadExecutor();

        incomingQueue = new ArrayBlockingQueue<>(queueSize);
        failedQueue = new ArrayBlockingQueue<>(queueSize);
        successQueue = new ArrayBlockingQueue<>(queueSize);
    }

    public synchronized void reset() {
        // clearing queue to enable multiple calls
    }

    public synchronized void execute(Iterable<Q> queries, int awaitTerminationHours, boolean retry) {
        Objects.requireNonNull(queries, "queries must not be null!");

        Supervisor supervisor = new Supervisor();
        supervisorExecutor.execute(supervisor);

        boolean success = false;
        int workerRetryCountdown = retry ? 1 : 0;
        while (workerRetryCountdown >= 0) {
            // clearing queues to enable multiple calls
            reset();

            ExecutorService workerExecutor = Executors.newFixedThreadPool(WORKER_THREAD_COUNT);
            supervisor.setWorkerExecutor(workerExecutor);

            // start all workers
            for (int i = 0; i < WORKER_THREAD_COUNT; i++) {
                supervisor.addNewWorker();
            }

            LOGGER.debug("Registered {} workers.", WORKER_THREAD_COUNT);

            for (Q query : queries) {
                QueryEntity preparedQuery = new QueryEntity(query); // NOPMD
                if (incomingQueue.contains(preparedQuery) || successQueue.contains(query)) {
                    LOGGER.info("Skipping already scheduled query {}", query);
                    continue;
                }

                try {
                    incomingQueue.put(preparedQuery);
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted!", e);
                    break;
                }
            }

            try {
                // signal that we are done.
                incomingQueue.put(sentinel);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted!", e);
            }

            try {
                // wait until all workers are done.
                // this means that every worker has seen the sentinel value
                workerExecutor.shutdown();

                LOGGER.debug("Waiting for shutdown of workers...");
                if (workerExecutor.awaitTermination(awaitTerminationHours, TimeUnit.HOURS)) {
                    LOGGER.debug("All workers are done!");
                    success = true;
                    break; // success
                }

                LOGGER.warn("Awaiting termination failed!");
                List<Runnable> incompleteTasks = workerExecutor.shutdownNow();
                if (LOGGER.isWarnEnabled())
                    LOGGER.warn("{} incomplete tasks: {}", incompleteTasks.size(), incompleteTasks);
            } catch (InterruptedException e) {
                LOGGER.warn("Awaiting termination interrupted!", e);
                break;
            } finally {
                LOGGER.debug("Stopping supervisor thread");
                supervisor.stop();
                supervisorExecutor.shutdownNow();
            }

            workerRetryCountdown--;
        }

        if (!success && LOGGER.isWarnEnabled()) {
            LOGGER.warn("Identified some incomplete worker tasks after retry");
        }

        if (!failedQueue.isEmpty()) {
            SqlCollectorException exception = new SqlCollectorException("Failed to execute!");

            for (QueryEntity queryEntity : failedQueue) {
                Throwable th = queryEntity.getThrowable();
                if (th != null) {
                    exception.addSuppressed(th);
                }
            }

            LOGGER.info("Processing done with some failed queries", exception);
            throw exception;
        }

        LOGGER.info("Processing done for all queries");
    }

    private class QueryEntity {
        private final Q query;
        private long timestamp;
        private long duration;
        private Long resultSize;
        private Throwable throwable;

        private QueryEntity() {
            query = null;
        }

        QueryEntity(Q query) {
            this.query = Objects.requireNonNull(query);
        }

        public Q getQuery() {
            return query;
        }

        public void setResultSize(Long resultSize) {
            this.resultSize = Objects.requireNonNull(resultSize, "resultSize must not be null!");
            this.throwable = null;
        }

        public long getResultSize() {
            return resultSize;
        }

        public void setThrowable(Throwable throwable) {
            Objects.requireNonNull(throwable, "throwable must not be null!");
            this.throwable = throwable;
            this.resultSize = null;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        public long getDuration() {
            return duration;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @SuppressWarnings("unchecked")
        @Override
        // does not include timestamp and duration on purpose
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryEntity that = (QueryEntity) o;
            return Objects.equals(query, that.query)
                && Objects.equals(resultSize, that.resultSize)
                && Objects.equals(throwable, that.throwable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query, timestamp, duration, resultSize, throwable);
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder(100)
                .append("QueryEntity{query=")
                .append(query)
                .append(", timestamp=").append(timestamp)
                .append(", duration=").append(duration);
            if (resultSize != null) {
                str.append(", resultSize=");
                str.append(resultSize);
            }
            if (throwable != null) {
                str.append(", throwable=").append(throwable);
            }
            str.append('}');
            return str.toString();
        }

        private boolean isSentinel() {
            // not a bug, check for instance equality, i.e. "same"!
            return SqlCollector.this.sentinel == this;
        }
    }

    private SqlClientBundle resolveClientBundle() {
        SqlClientBundle result = clientBundleThreadLocal.get();
        if (result == null) {
            // this happens in case of initial call or after the client has been reset
            // via call to resetClientBundle()
            result = clientFactory.createInstance(dataSource);
            clientBundleThreadLocal.set(result);
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Created fresh client with data-source {}.", result.getDataSource());
        }

        return result;
    }

    private void resetClientBundle() {
        SqlClientBundle clientBundle = clientBundleThreadLocal.get();
        if (clientBundle == null) {
            return;
        }

        // perform cleanup
        clientBundleThreadLocal.set(null);
        try {
            DataSource dataSource = clientBundle.getDataSource();
            if (dataSource != null) {
                Connection conn = dataSource.getConnection(); // NOPMD
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            }
            LOGGER.debug("Closed client.");
        } catch (Throwable t) {
            LOGGER.warn("Exception while closing client!", t);
        }
    }

    private class Supervisor implements Runnable {
        private final Set<Worker> workers = new HashSet<>();

        private ExecutorService workerExecutor;

        private final AtomicBoolean running = new AtomicBoolean(true);

        public void setWorkerExecutor(ExecutorService workerExecutor) {
            this.workerExecutor = Objects.requireNonNull(workerExecutor, "workerExecutor must ot be null!");
        }

        public void stop() {
            running.set(false);
        }

        public void addNewWorker() {
            LOGGER.info("Adding a new worker");
            Worker worker = new Worker();
            workerExecutor.execute(worker);
            workers.add(worker);
        }

        @Override
        public void run() {
            long maxDuration = sqlQuery.getMaxExecutionTime();
            if (maxDuration == -1) {
                LOGGER.warn("Max duration check is disabled!");
            }

            while (running.get()) {
                try {
                    if (!workers.isEmpty() && workerExecutor != null) {
                        LOGGER.info("Starting worker verification");

                        long current = System.currentTimeMillis();

                        for (Worker worker : workers) {
                            QueryExecutionContext ctx = worker.getContext();
                            Long workerStartTimestamp = ctx.getStartTimestamp();
                            if (workerStartTimestamp == null) {
                                if (worker.isDone()) {
                                    LOGGER.debug("Worker is done");
                                }
                                else {
                                    LOGGER.debug("Worker is waiting or starting a new query");
                                }
                                continue;
                            }

                            long duration = current - workerStartTimestamp;
                            String durationValue = DurationFormatter.fromMillis(duration).format("%2m:%2s");
                            if (maxDuration == -1 || duration <= maxDuration) {
                                LOGGER.debug("Worker is executing query, current duration {} :{}",
                                    durationValue, worker.currentQuery);
                                continue;
                            }

                            String maxDurationValue = DurationFormatter.fromMillis(maxDuration).format("%2m:%2s");
                            LOGGER.info("Worker execution time {} exceeds specified timeout of {} and will be cancelled: {}",
                                durationValue, maxDurationValue, worker.currentQuery);

                            ctx.cancel();
                        }
                    } else {
                        LOGGER.info("Waiting for initializing...");
                    }

                    try {
                        Thread.sleep(SUPERVISOR_CHECK_INTERVAL);
                    } catch (InterruptedException ex) {
                        LOGGER.warn("Closing supervisor after an interrupt!", ex);
                        return;
                    }
                } catch (Exception ex) {
                    LOGGER.error("Unexpected exception occurred!", ex);
                }
            }
        }
    }

    private class Worker implements Runnable {

        private final QueryExecutionContext context = new QueryExecutionContext();
        private Q currentQuery = null;
        private boolean done = false;

        @Override
        public void run() {
            while (true) {
                try {
                    QueryEntity queryEntity = incomingQueue.take();

                    // check sentinel value
                    if (queryEntity.isSentinel()) {
                        // we are done.
                        incomingQueue.put(queryEntity); // put it back for other workers...
                        // failedQueue.put(queryEntity); // put it also to failed queue
                        done = true;
                        if (LOGGER.isDebugEnabled()) LOGGER.debug("Thread {} is done.", Thread.currentThread());
                        break;
                    }

                    Q query = queryEntity.getQuery();

                    if (LOGGER.isInfoEnabled()) {
                        if (query == null) {
                            LOGGER.info("Remaining queries in the incoming queue: {}", incomingQueue.size()); // NOPMD
                        } else {
                            LOGGER.info("Start processing {}, remaining queries in the incoming queue: {}", query, incomingQueue.size()); // NOPMD
                        }
                    }

                    long startQueryTimestamp = System.currentTimeMillis();
                    context.setStartTimestamp(startQueryTimestamp);
                    context.setCancelled(false);
                    currentQuery = query;
                    queryEntity.setTimestamp(startQueryTimestamp);
                    long startTime = System.nanoTime(); // NOPMD
                    ProcessingState state = ProcessingState.createSuccessState();
                    try (Drain<R> drain = drainFactory.createDrain(query)) {
                        long count = executeQuery(query, drain, metricsDrain, context);
                        LOGGER.debug("Collected {} entries", count);
                        if (count == -1) {
                            // cancelled query
                            throw new IllegalStateException("Query cancelled: " + query);
                        }
                        if (query != null) {
                            successQueue.add(query);
                        }
                    } catch (Throwable e) {
                        LOGGER.warn("Exception while performing query {}!", queryEntity, e);
                        queryEntity.setThrowable(e);
                        failedQueue.put(queryEntity);

                        // write error state-file
                        state = ProcessingState.createFailedState("Error processing query " + query, e);
                    } finally {
                        context.clear();
                        currentQuery = null;

                        if (processingStateFileFactory != null) {
                            File stateFile = processingStateFileFactory.getProcessingStateFile(query);
                            ProcessingStateFileWriter.write(stateFile, state);
                        }
                    }
                    queryEntity.setDuration((System.nanoTime() - startTime) / MILLIS_IN_NANO);
                } catch (InterruptedException ex) {
                    LOGGER.warn("Interrupted!", ex);
                    break;
                }
            }

            // get rid of lingering client since we are done
            resetClientBundle();
        }

        public Q getCurrentQuery() {
            return currentQuery;
        }

        public boolean isDone() {
            return done;
        }

        public QueryExecutionContext getContext() {
            return context;
        }

        private long executeQuery(
            Q query,
            Drain<R> resultDrain,
            Drain<SqlMetrics<Q>> metricsDrain,
            QueryExecutionContext context
        ) {
            Throwable throwable = null;
            for (int i = 0; i < sqlQuery.getMaxRetries(); i++) {
                try {
                    SqlClientBundle clientBundle = resolveClientBundle();
                    return sqlQuery.perform(clientBundle, query, resultDrain, metricsDrain, context);
                } catch (Throwable e) {
                    throwable = e;
                    LOGGER.warn("Failed to perform query {}!", query, throwable);
                }
                // get rid of previous client after an error occurred
                // next call to resolveClient returns a fresh instance
                resetClientBundle();
            }

            if (LOGGER.isWarnEnabled()) LOGGER.warn("Bailing out after {} retries: {}", sqlQuery.getMaxRetries(), query);
            throw new SqlCollectorException("Failed to execute " + query + "!", throwable);
        }
    }

    public static class QueryExecutionContext {
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private Long startTimestamp;
        private PreparedStatement statement;

        public void clear() {
            cancelled.set(false);
            startTimestamp = null;
            statement = null;
        }

        public void cancel() throws SQLException {
            cancelled.set(true);
            if (statement != null) {
                statement.cancel();
            }
        }

        public boolean isCancelled() {
            return cancelled.get();
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled.set(cancelled);
        }

        public Long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(Long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public PreparedStatement getStatement() {
            return statement;
        }

        public void setStatement(PreparedStatement statement) {
            this.statement = statement;
        }
    }

    public static class SqlMetrics<Q> {
        public static final String COLUMN_ID = "ID";
        public static final String COLUMN_RESULT_COUNT = "RESULT_COUNT";
        public static final String COLUMN_START_TIMESTAMP = "START_TIMESTAMP";
        public static final String COLUMN_INIT_DURATION = "INIT_DURATION";
        public static final String COLUMN_DELIVERY_DURATION = "DELIVERY_DURATION";
        public static final String COLUMN_END_TIMESTAMP = "END_TIMESTAMP";
        public static final String COLUMN_QUERY = "QUERY";
        public static final String COLUMN_SUCCESS = "SUCCESS";
        public static final String COLUMN_MESSAGE = "MESSAGE";

        public static final String[] COLUMNS = {
            COLUMN_ID,
            COLUMN_RESULT_COUNT,
            COLUMN_START_TIMESTAMP,
            COLUMN_INIT_DURATION,
            COLUMN_DELIVERY_DURATION,
            COLUMN_END_TIMESTAMP,
            COLUMN_QUERY,
            COLUMN_SUCCESS,
            COLUMN_MESSAGE,
        };

        private final String id;
        private final long resultCount;
        private final LocalDateTime startTimestamp;
        private final long initDuration;
        private final long deliveryDuration;
        private final LocalDateTime endTimestamp;
        private final Q query;
        private final boolean success;
        private final String message;

        public SqlMetrics(
            String id,
            long resultCount,
            LocalDateTime startTimestamp,
            long initDuration,
            long deliveryDuration,
            LocalDateTime endTimestamp,
            Q query
        ) {
            this.id = Objects.requireNonNull(id, "id must not be null!");
            this.resultCount = resultCount;
            this.startTimestamp = Objects.requireNonNull(startTimestamp, "startTimestamp must not be null!");
            this.initDuration = initDuration;
            this.deliveryDuration = deliveryDuration;
            this.endTimestamp = Objects.requireNonNull(endTimestamp, "endTimestamp must not be null!");
            this.query = Objects.requireNonNull(query, "query must not be null!");
            this.success = true;
            this.message = null;
        }

        public SqlMetrics(
            String id,
            long resultCount,
            LocalDateTime startTimestamp,
            long initDuration,
            long deliveryDuration,
            LocalDateTime endTimestamp,
            Q query,
            boolean success,
            String message
        ) {
            this.id = Objects.requireNonNull(id, "id must not be null!");
            this.resultCount = resultCount;
            this.startTimestamp = Objects.requireNonNull(startTimestamp, "startTimestamp must not be null!");
            this.initDuration = initDuration;
            this.deliveryDuration = deliveryDuration;
            this.endTimestamp = Objects.requireNonNull(endTimestamp, "endTimestamp must not be null!");
            this.query = Objects.requireNonNull(query, "query must not be null!");
            this.success = success;
            this.message = message;
        }

        public String getId() {
            return id;
        }

        public long getResultCount() {
            return resultCount;
        }

        public LocalDateTime getStartTimestamp() {
            return startTimestamp;
        }

        public long getInitDuration() {
            return initDuration;
        }

        public long getDeliveryDuration() {
            return deliveryDuration;
        }

        public LocalDateTime getEndTimestamp() {
            return endTimestamp;
        }

        public Q getQuery() {
            return query;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SqlMetrics)) return false;
            SqlMetrics<?> that = (SqlMetrics<?>) o;
            return resultCount == that.resultCount
                && initDuration == that.initDuration
                && deliveryDuration == that.deliveryDuration
                && success == that.success && Objects.equals(id, that.id)
                && Objects.equals(startTimestamp, that.startTimestamp)
                && Objects.equals(endTimestamp, that.endTimestamp)
                && Objects.equals(query, that.query)
                && Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, resultCount, startTimestamp, initDuration, deliveryDuration, endTimestamp, query, success, message);
        }

        @Override
        public String toString() {
            return "SqlMetrics{" +
                "id='" + id + '\'' +
                ", resultCount=" + resultCount +
                ", startTimestamp=" + startTimestamp +
                ", initDuration=" + initDuration +
                ", deliveryDuration=" + deliveryDuration +
                ", endTimestamp=" + endTimestamp +
                ", query=" + query +
                ", success=" + success +
                ", message='" + message + '\'' +
                '}';
        }
    }
}
