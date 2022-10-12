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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
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

    private final AtomicBoolean failure = new AtomicBoolean();
    private final ThreadLocal<SqlClientBundle> clientBundleThreadLocal = new ThreadLocal<>();
    private final BlockingQueue<QueryEntity> incomingQueue;
    private final BlockingQueue<QueryEntity> failedQueue;
    private final QueryEntity sentinel = new QueryEntity();
    private final SqlClientFactory clientFactory;
    private final SqlQuery<Q, R> sqlQuery;
    private final DrainFactory<Q, R> drainFactory;
    private final Drain<SqlMetrics<Q>> metricsDrain;
    private final DataSource dataSource;
    private final int maxRetries;

    public SqlCollector(
        DataSource dataSource,
        SqlClientFactory clientFactory,
        SqlQuery<Q, R> sqlQuery,
        DrainFactory<Q, R> drainFactory,
        Drain<SqlMetrics<Q>> metricsDrain,
        int queueSize
    ) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null!");
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory must not be null!");
        this.sqlQuery = Objects.requireNonNull(sqlQuery, "sqlQuery must not be null!");
        this.drainFactory = Objects.requireNonNull(drainFactory, "drainFactory must not be null!");
        this.metricsDrain = metricsDrain;

        incomingQueue = new ArrayBlockingQueue<>(queueSize);
        failedQueue = new ArrayBlockingQueue<>(queueSize);

        this.maxRetries = 5; // TODO: parameter, check
    }

    public synchronized void reset() {
        // clearing queue to enable multiple calls
        failure.set(false);
    }

    public synchronized void execute(Iterable<Q> queries, int awaitTerminationHours, boolean retry) {
        Objects.requireNonNull(queries, "queries must not be null!");

        boolean success = false;
        int workerRetryCountdown = retry ? 1 : 0;
        while (workerRetryCountdown >= 0) {
            // clearing queues to enable multiple calls
            reset();

            ExecutorService executorService = createExecutorService();

            for (Q query : queries) {
                if (failure.get()) {
                    break;
                }

                try {
                    incomingQueue.put(new QueryEntity(query));
                } catch (InterruptedException e) {
                    if (LOGGER.isWarnEnabled()) LOGGER.warn("Interrupted!", e);
                    break;
                }
            }

            try {
                // signal that we are done.
                incomingQueue.put(sentinel);
            } catch (InterruptedException e) {
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Interrupted!", e);
            }

            try {
                // wait until all workers are done.
                // this means that every worker has seen the sentinel value
                executorService.shutdown();

                if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for shutdown of workers...");
                if (executorService.awaitTermination(awaitTerminationHours, TimeUnit.HOURS)) {
                    if (LOGGER.isDebugEnabled()) LOGGER.debug("All workers are done!");
                    success = true;
                    break; // success
                }

                if (LOGGER.isWarnEnabled()) LOGGER.warn("Awaiting termination failed!");
                List<Runnable> incompleteTasks = executorService.shutdownNow();
                if (LOGGER.isWarnEnabled())
                    LOGGER.warn("{} incomplete tasks: {}", incompleteTasks.size(), incompleteTasks);
            } catch (InterruptedException e) {
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Awaiting termination interrupted!", e);
                break;
            }

            workerRetryCountdown--;
        }

        if (!success && LOGGER.isWarnEnabled()) {
            LOGGER.warn("Identified some incomplete worker tasks after retry");
        }

        if (failure.get()) {
            SqlCollectorException exception = new SqlCollectorException("Failed to execute!");

            for (; ; ) {
                QueryEntity queryEntity;
                try {
                    queryEntity = failedQueue.take();
                } catch (InterruptedException ex) {
                    if (LOGGER.isWarnEnabled()) LOGGER.warn("Interrupted!", ex);
                    break;
                }

                Throwable th = queryEntity.getThrowable();
                if (th != null) {
                    exception.addSuppressed(th);
                }
            }
            throw exception;
        }
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private ExecutorService createExecutorService() {
        ExecutorService result = Executors.newFixedThreadPool(WORKER_THREAD_COUNT);
        for (int i = 0; i < WORKER_THREAD_COUNT; i++) {
            result.execute(new Worker());
        }

        if (LOGGER.isDebugEnabled()) LOGGER.debug("Registered {} workers.", WORKER_THREAD_COUNT);
        return result;
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

    @SuppressWarnings("PMD.CloseResource")
    private long executeQuery(Q query, Drain<R> resultDrain, Drain<SqlMetrics<Q>> metricsDrain) {
        Throwable throwable = null;
        for (int i = 0; i < maxRetries; i++) {
            try {
                SqlClientBundle clientBundle = resolveClientBundle();
                return sqlQuery.perform(clientBundle, query, resultDrain, metricsDrain);
            } catch (Throwable e) {
                throwable = e;
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Failed to perform query {}!", query, throwable);
            }
            // get rid of previous client after an error occurred
            // next call to resolveClient returns a fresh instance
            resetClientBundle();
        }
        if (LOGGER.isWarnEnabled()) LOGGER.warn("Bailing out after {} retries: {}", maxRetries, query);
        throw new SqlCollectorException("Failed to execute " + query + "!", throwable);
    }

    private SqlClientBundle resolveClientBundle() {
        SqlClientBundle result = clientBundleThreadLocal.get();
        if (result == null) {
            // this happens in case of initial call or after the client has been reset
            // via call to resetClientBundle()
            result = clientFactory.createInstance(dataSource);
            clientBundleThreadLocal.set(result);
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Created fresh client with data-source {}.", result.getDataSource());
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
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Closed client.");
        } catch (Throwable t) {
            if (LOGGER.isWarnEnabled()) LOGGER.warn("Exception while closing client!", t);
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                try {
                    QueryEntity queryEntity = incomingQueue.take();
                    if (LOGGER.isInfoEnabled())
                        LOGGER.info("Start processing {}, queries remaining in the incoming queue: {}", queryEntity.getQuery(), incomingQueue.size());

                    // check sentinel value
                    if (queryEntity.isSentinel()) {
                        // we are done.
                        incomingQueue.put(queryEntity); // put it back for other workers...
                        failedQueue.put(queryEntity); // put it also to failed queue
                        if (LOGGER.isDebugEnabled()) LOGGER.debug("Thread {} is done.", Thread.currentThread());
                        break;
                    }

                    if (failure.get()) {
                        // just ignore any other work
                        break;
                    }

                    queryEntity.setTimestamp(System.currentTimeMillis());
                    long startTime = System.nanoTime();
                    Q query = queryEntity.getQuery();
                    try (Drain<R> drain = drainFactory.createDrain(query)) {
                        long count = executeQuery(query, drain, metricsDrain);
                        LOGGER.debug("Collected {} entries", count);
                    } catch (Throwable e) {
                        if (LOGGER.isWarnEnabled()) LOGGER.warn("Exception while performing query {}!", queryEntity, e);
                        failure.set(true);
                        queryEntity.setThrowable(e);
                        failedQueue.put(queryEntity);
                    }
                    queryEntity.setDuration((System.nanoTime() - startTime) / MILLIS_IN_NANO);
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted!", e);
                    break;
                }
            }

            // get rid of lingering client since we are done
            resetClientBundle();
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

        public static final String[] COLUMNS = {
            COLUMN_ID,
            COLUMN_RESULT_COUNT,
            COLUMN_START_TIMESTAMP,
            COLUMN_INIT_DURATION,
            COLUMN_DELIVERY_DURATION,
            COLUMN_END_TIMESTAMP,
            COLUMN_QUERY,
        };

        private final String id;
        private final long resultCount;
        private final LocalDateTime startTimestamp;
        private final long initDuration;
        private final long deliveryDuration;
        private final LocalDateTime endTimestamp;
        private final Q query;

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SqlMetrics)) return false;
            SqlMetrics<?> that = (SqlMetrics<?>) o;
            return resultCount == that.resultCount
                && initDuration == that.initDuration
                && deliveryDuration == that.deliveryDuration
                && Objects.equals(id, that.id)
                && Objects.equals(startTimestamp, that.startTimestamp)
                && Objects.equals(endTimestamp, that.endTimestamp)
                && Objects.equals(query, that.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, resultCount, startTimestamp, initDuration, deliveryDuration, endTimestamp, query);
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
                '}';
        }
    }
}
