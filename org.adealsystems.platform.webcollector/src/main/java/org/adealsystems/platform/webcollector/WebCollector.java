/*
 * Copyright 2020-2021 ADEAL Systems GmbH
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

package org.adealsystems.platform.webcollector;

import org.adealsystems.platform.io.Drain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WebCollector<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebCollector.class);
    private static final int MILLIS_IN_NANO = 1_000_000;

    private final AtomicBoolean failure = new AtomicBoolean();
    private final ThreadLocal<HttpClientBundle> clientBundleThreadLocal = new ThreadLocal<>();
    private final BlockingQueue<QueryEntity> incomingQueue;
    private final BlockingQueue<QueryEntity> doneQueue;
    private final QueryEntity sentinel = new QueryEntity();
    private final HttpClientFactory clientFactory;
    private final HttpQuery<Q, R> httpQuery;
    private final int threadCount;
    private final int maxRetries;

    public WebCollector(HttpClientFactory clientFactory, HttpQuery<Q, R> httpQuery, int queueSize) {
        this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory must not be null!");
        this.httpQuery = Objects.requireNonNull(httpQuery, "httpQuery must not be null!");
        incomingQueue = new ArrayBlockingQueue<>(queueSize);
        doneQueue = new ArrayBlockingQueue<>(queueSize);

        this.threadCount = 8; // TODO: parameter, check
        this.maxRetries = 5; // TODO: parameter, check
    }

    public synchronized void reset() {
        // clearing queues to enable multiple calls
        incomingQueue.clear();
        doneQueue.clear();
        failure.set(false);
    }

    public synchronized void execute(Iterable<Q> queries, Drain<R> resultDrain) {
        execute(queries, resultDrain, null);
    }

    public synchronized void execute(Iterable<Q> queries, Drain<R> resultDrain, Drain<Metrics<Q>> metricsDrain) {
        Objects.requireNonNull(queries, "queries must not be null!");

        // clearing queues to enable multiple calls
        reset();

        ExecutorService producerExecutorService = createProducerExecutorService();

        Consumer consumer = new Consumer(resultDrain, metricsDrain);
        Thread consumerThread = new Thread(consumer, "WebCollector-Consumer-Thread");
        consumerThread.start();
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Started consumer thread.");

        long startTime = System.nanoTime();
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
            // wait until all producers are done.
            // this means that every producer has seen the sentinel value
            producerExecutorService.shutdown();

            if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for shutdown of producers...");
            if (!producerExecutorService.awaitTermination(1, TimeUnit.DAYS)) {
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Awaiting termination failed!");
            } else {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("All producers are done!");
            }
        } catch (InterruptedException e) {
            if (LOGGER.isWarnEnabled()) LOGGER.warn("Awaiting termination interrupted!", e);
        }

        // no more results will show up.
        // add sentinel value to doneQueue.
        try {
            doneQueue.put(sentinel);
        } catch (InterruptedException e) {
            if (LOGGER.isWarnEnabled()) LOGGER.warn("Interrupted while adding sentinel value!", e);
        }

        try {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for shutdown of consumer thread...");
            consumerThread.join();
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Consumer is done!");
        } catch (InterruptedException e) {
            if (LOGGER.isWarnEnabled()) LOGGER.warn("Interrupted while waiting for consumer thread!", e);
        }
        if (LOGGER.isInfoEnabled()) {
            long absoluteMillis = (System.nanoTime() - startTime) / MILLIS_IN_NANO;
            long accumulatedMillis = consumer.getAccumulatedMillis();
            int percentage = (int) (((100.0d / accumulatedMillis) * absoluteMillis) + 0.5d);
            int totalQueries = consumer.getTotalQueries();
            int failedQueries = consumer.getFailedQueries();
            int totalResults = consumer.getTotalResults();
            long averageMillisPerQueryAbsolute = absoluteMillis / totalQueries;
            long averageMillisPerQueryAccumulated = accumulatedMillis / totalQueries;

            LOGGER.info("Accumulated execution time: {}ms", accumulatedMillis);
            LOGGER.info("Absolute execution time   : {}ms", absoluteMillis);
            LOGGER.info("Accumulated vs. Absolute  : {}%", percentage);
            LOGGER.info("Total queries             : {}", totalQueries);
            LOGGER.info("Failed queries            : {}", failedQueries);
            LOGGER.info("Total results             : {}", totalResults);
            LOGGER.info("Average per Query (abs)   : {}ms", averageMillisPerQueryAbsolute);
            LOGGER.info("Average per Query (acc)   : {}ms", averageMillisPerQueryAccumulated);
        }
        if (failure.get()) {
            WebCollectorException exception = new WebCollectorException("Failed to execute!");
            for (Throwable t : consumer.getThrowables()) {
                exception.addSuppressed(t);
            }
            throw exception;
        }
    }

    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    private ExecutorService createProducerExecutorService() {
        ExecutorService result = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            result.execute(new Producer());
        }
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Registered {} producers.", threadCount);
        return result;
    }


    private class QueryEntity {
        private final Q query;
        private long duration;
        private List<R> result;
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

        public void setResult(List<R> result) {
            Objects.requireNonNull(result, "result must not be null!");
            this.result = result;
            this.throwable = null;
        }

        public List<R> getResult() {
            return result;
        }

        public void setThrowable(Throwable throwable) {
            Objects.requireNonNull(throwable, "throwable must not be null!");
            this.throwable = throwable;
            this.result = null;
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

        @SuppressWarnings("unchecked")
        @Override
        // does not include duration on purpose
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryEntity other = (QueryEntity) o;
            return Objects.equals(query, other.query) && Objects.equals(result, other.result) && Objects.equals(throwable, other.throwable);
        }

        @Override
        // does not include duration on purpose
        public int hashCode() {
            return Objects.hash(query, result, throwable);
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder("QueryEntity{query=")
                .append(query)
                .append(", duration=").append(duration);
            if (result != null) {
                str.append(", result.size()=");
                str.append(result.size());
            }
            if (throwable != null) {
                str.append(", throwable=").append(throwable);
            }
            str.append('}');
            return str.toString();
        }

        private boolean isSentinel() {
            // not a bug, check for instance equality, i.e. "same"!
            return WebCollector.this.sentinel == this;
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private List<R> executeQuery(Q query) {
        Throwable throwable = null;
        for (int i = 0; i < maxRetries; i++) {
            try {
                HttpClientBundle clientBundle = resolveClientBundle();
                return httpQuery.perform(clientBundle, query);
            } catch (Throwable e) {
                throwable = e;
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Failed to perform query {}!", query, throwable);
            }
            // get rid of previous client after an error occurred
            // next call to resolveClient returns a fresh instance
            resetClientBundle();
        }
        if (LOGGER.isWarnEnabled()) LOGGER.warn("Bailing out after {} retries: {}", maxRetries, query);
        throw new WebCollectorException("Failed to execute " + query + "!", throwable);
    }

    private HttpClientBundle resolveClientBundle() {
        HttpClientBundle result = clientBundleThreadLocal.get();
        if (result == null) {
            // this happens in case of initial call or after the client has been reset
            // via call to resetClientBundle()
            result = clientFactory.createInstance();
            clientBundleThreadLocal.set(result);
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Created fresh client.");
        }

        return result;
    }

    private void resetClientBundle() {
        HttpClientBundle clientBundle = clientBundleThreadLocal.get();
        if (clientBundle == null) {
            return;
        }
        // perform cleanup
        clientBundleThreadLocal.set(null);
        try {
            clientBundle.getClient().close();
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Closed client.");
        } catch (Throwable t) {
            if (LOGGER.isWarnEnabled()) LOGGER.warn("Exception while closing client!", t);
        }
    }

    private class Producer implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                try {
                    QueryEntity queryEntity = incomingQueue.take();
                    // check sentinel value
                    if (queryEntity.isSentinel()) {
                        // we are done.
                        incomingQueue.put(queryEntity); // put it back for other workers...
                        if (LOGGER.isDebugEnabled()) LOGGER.debug("Thread {} is done.", Thread.currentThread());
                        break;
                    }
                    if (failure.get()) {
                        // just ignore any other work
                        break;
                    }
                    long startTime = System.nanoTime();
                    try {
                        queryEntity.setResult(executeQuery(queryEntity.getQuery()));
                    } catch (Throwable e) {
                        if (LOGGER.isWarnEnabled()) LOGGER.warn("Exception while performing query {}!", queryEntity, e);
                        failure.set(true);
                        queryEntity.setThrowable(e);
                    }
                    queryEntity.setDuration((System.nanoTime() - startTime) / MILLIS_IN_NANO);

                    doneQueue.put(queryEntity);
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted!", e);
                    break;
                }
            }

            // get rid of lingering client since we are done
            resetClientBundle();
        }
    }

    private class Consumer implements Runnable {
        private final Drain<R> resultDrain;
        private final Drain<Metrics<Q>> metricsDrain;
        private final List<Throwable> throwables = new ArrayList<>();
        private long accumulatedMillis = 0;
        private int totalQueries = 0;
        private int failedQueries = 0;
        private int totalResults = 0;

        Consumer(Drain<R> resultDrain, Drain<Metrics<Q>> metricsDrain) {
            this.resultDrain = Objects.requireNonNull(resultDrain, "resultDrain must not be null!");
            this.metricsDrain = metricsDrain; // can be null
        }

        public List<Throwable> getThrowables() {
            return throwables;
        }

        public long getAccumulatedMillis() {
            return accumulatedMillis;
        }

        public int getTotalQueries() {
            return totalQueries;
        }

        public int getFailedQueries() {
            return failedQueries;
        }

        public int getTotalResults() {
            return totalResults;
        }

        @Override
        public void run() {
            for (; ; ) {
                try {
                    QueryEntity queryEntity = doneQueue.take();
                    // check sentinel value
                    if (queryEntity.isSentinel()) {
                        // we are done.
                        if (LOGGER.isDebugEnabled()) LOGGER.debug("Thread {} is done.", Thread.currentThread());
                        return;
                    }
                    totalQueries++;
                    accumulatedMillis += queryEntity.getDuration();
                    Throwable throwable = queryEntity.getThrowable();
                    if (throwable != null) {
                        throwables.add(throwable);
                        failedQueries++;
                        continue;
                    }

                    List<R> result = queryEntity.getResult();
                    if (result != null) {
                        resultDrain.addAll(result);
                        totalResults += result.size();
                        if (LOGGER.isDebugEnabled())
                            LOGGER.debug("Drained {} results for query {}.", result.size(), queryEntity);

                        drainMetrics(queryEntity, metricsDrain);
                    }
                } catch (InterruptedException e) {
                    if (LOGGER.isWarnEnabled()) LOGGER.warn("Interrupted!", e);
                    return;
                }
            }
        }

        private void drainMetrics(QueryEntity queryEntity, Drain<Metrics<Q>> metricsDrain) {
            if (metricsDrain == null) {
                return;
            }
            List<R> result = queryEntity.getResult();
            if (result == null) {
                return;
            }
            Metrics<Q> metrics = new Metrics<>(result.size(), queryEntity.getDuration(), queryEntity.getQuery());
            try {
                metricsDrain.add(metrics);
            } catch (Throwable t) {
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Failed to drain metrics! Continuing anyway...", t);
            }
        }
    }

    public static class Metrics<Q> {
        private int resultCount;
        private long duration;
        private Q query;

        public Metrics(int resultCount, long duration, Q query) {
            this.resultCount = resultCount;
            this.duration = duration;
            this.query = query;
        }

        public int getResultCount() {
            return resultCount;
        }

        public void setResultCount(int resultCount) {
            this.resultCount = resultCount;
        }

        public long getDuration() {
            return duration;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }

        public Q getQuery() {
            return query;
        }

        public void setQuery(Q query) {
            this.query = query;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Metrics<?> metrics = (Metrics<?>) o;
            return resultCount == metrics.resultCount && duration == metrics.duration && Objects.equals(query, metrics.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resultCount, duration, query);
        }

        @Override
        public String toString() {
            return "Metrics{" +
                "resultCount=" + resultCount +
                ", duration=" + duration +
                ", query=" + query +
                '}';
        }
    }
}

