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

package org.adealsystems.platform.webcollector

import org.adealsystems.platform.io.ListDrain
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicInteger

class WebCollectorSpec extends Specification {
    def "collect some stuff"() {
        given:
        HttpClientFactory clientFactory = new DefaultHttpClientFactory()
        HttpQuery<QueryType, ResultType> query = new StableQuery()
        WebCollector<QueryType, ResultType> collector = new WebCollector<>(clientFactory, query, 100)
        List<QueryType> queries = new ArrayList()
        queries.add(new QueryType("query-0"))
        queries.add(new QueryType("query-1"))
        queries.add(new QueryType("query-2"))
        queries.add(new QueryType("query-3"))
        queries.add(new QueryType("query-4"))
        ListDrain<ResultType> resultDrain = new ListDrain<>()

        when:
        collector.execute(queries, resultDrain)

        then:
        resultDrain.getContent().size() == 50
    }

    def "collect some stuff with metrics"() {
        given:
        HttpClientFactory clientFactory = new DefaultHttpClientFactory()
        HttpQuery<QueryType, ResultType> query = new StableQuery()
        WebCollector<QueryType, ResultType> collector = new WebCollector<>(clientFactory, query, 100)
        List<QueryType> queries = new ArrayList()
        queries.add(new QueryType("query-0"))
        queries.add(new QueryType("query-1"))
        queries.add(new QueryType("query-2"))
        queries.add(new QueryType("query-3"))
        queries.add(new QueryType("query-4"))
        ListDrain<ResultType> resultDrain = new ListDrain<>()
        ListDrain<WebCollector.Metrics<QueryType>> metricsDrain = new ListDrain<>()

        when:
        collector.execute(queries, resultDrain, metricsDrain)

        then:
        resultDrain.getContent().size() == 50
        metricsDrain.getContent().size() == 5
    }

    def "collect some flaky stuff"() {
        given:
        HttpClientFactory clientFactory = new DefaultHttpClientFactory()
        HttpQuery<QueryType, ResultType> query = new FlakyQuery()
        WebCollector<QueryType, ResultType> collector = new WebCollector<>(clientFactory, query, 100)
        List<QueryType> queries = new ArrayList()
        queries.add(new QueryType("query-0"))
        queries.add(new QueryType("query-1"))
        queries.add(new QueryType("query-2"))
        queries.add(new QueryType("query-3"))
        queries.add(new QueryType("query-4"))
        ListDrain<ResultType> resultDrain = new ListDrain<>()

        when:
        collector.execute(queries, resultDrain)

        then:
        resultDrain.getContent().size() == 50
    }

    def "collect some flaky stuff with metrics"() {
        given:
        HttpClientFactory clientFactory = new DefaultHttpClientFactory()
        HttpQuery<QueryType, ResultType> query = new FlakyQuery()
        WebCollector<QueryType, ResultType> collector = new WebCollector<>(clientFactory, query, 100)
        List<QueryType> queries = new ArrayList()
        queries.add(new QueryType("query-0"))
        queries.add(new QueryType("query-1"))
        queries.add(new QueryType("query-2"))
        queries.add(new QueryType("query-3"))
        queries.add(new QueryType("query-4"))
        ListDrain<ResultType> resultDrain = new ListDrain<>()
        ListDrain<WebCollector.Metrics<QueryType>> metricsDrain = new ListDrain<>()

        when:
        collector.execute(queries, resultDrain, metricsDrain)

        then:
        resultDrain.getContent().size() == 50
        metricsDrain.getContent().size() == 5
    }

    def "collect some broken stuff"() {
        given:
        HttpClientFactory clientFactory = new DefaultHttpClientFactory()
        HttpQuery<QueryType, ResultType> query = new BrokenQuery()
        WebCollector<QueryType, ResultType> collector = new WebCollector<>(clientFactory, query, 100)
        List<QueryType> queries = new ArrayList()
        queries.add(new QueryType("query-0"))
        queries.add(new QueryType("query-1"))
        queries.add(new QueryType("query-2"))
        queries.add(new QueryType("query-3"))
        queries.add(new QueryType("query-4"))
        ListDrain<ResultType> resultDrain = new ListDrain<>()

        when:
        collector.execute(queries, resultDrain)

        then:
        WebCollectorException ex = thrown()
        ex.message == "Failed to execute!"
        ex.suppressed.length == 2
        and:
        resultDrain.getContent().size() == 30
    }

    def "collect some broken stuff with metrics"() {
        given:
        HttpClientFactory clientFactory = new DefaultHttpClientFactory()
        HttpQuery<QueryType, ResultType> query = new BrokenQuery()
        WebCollector<QueryType, ResultType> collector = new WebCollector<>(clientFactory, query, 100)
        List<QueryType> queries = new ArrayList()
        queries.add(new QueryType("query-0"))
        queries.add(new QueryType("query-1"))
        queries.add(new QueryType("query-2"))
        queries.add(new QueryType("query-3"))
        queries.add(new QueryType("query-4"))
        ListDrain<ResultType> resultDrain = new ListDrain<>()
        ListDrain<WebCollector.Metrics<QueryType>> metricsDrain = new ListDrain<>()

        when:
        collector.execute(queries, resultDrain, metricsDrain)

        then:
        WebCollectorException ex = thrown()
        ex.message == "Failed to execute!"
        ex.suppressed.length == 2
        and:
        resultDrain.getContent().size() == 30
        metricsDrain.getContent().size() == 3
    }

    static class StableQuery implements HttpQuery<QueryType, ResultType> {

        @Override
        List<ResultType> perform(HttpClientBundle httpClientBundle, QueryType query) throws IOException {
            try {
                // simulate some latency
                Thread.currentThread().sleep(50)
            } catch (InterruptedException ignore) {
                // ignore
            }
            List<ResultType> result = new ArrayList<>()
            for (int i = 0; i < 10; i++) {
                def resultValue = new ResultType(query.getId(), query.getId() + "-value-" + i)

                result.add(resultValue)
            }
            return result
        }
    }

    static class FlakyQuery implements HttpQuery<QueryType, ResultType> {
        private final AtomicInteger counter = new AtomicInteger()

        @Override
        List<ResultType> perform(HttpClientBundle httpClientBundle, QueryType query) throws IOException {
            try {
                // simulate some latency
                Thread.currentThread().sleep(50)
            } catch (InterruptedException ignore) {
                // ignore
            }
            int count = counter.incrementAndGet()
            if (count % 3 == 0) {
                throw new IOException("Flaky connection!")
            }
            List<ResultType> result = new ArrayList<>()
            for (int i = 0; i < 10; i++) {
                def resultValue = new ResultType(query.getId(), query.getId() + "-value-" + i)

                result.add(resultValue)
            }
            return result
        }
    }

    static class BrokenQuery implements HttpQuery<QueryType, ResultType> {
        private final AtomicInteger counter = new AtomicInteger()

        @Override
        List<ResultType> perform(HttpClientBundle httpClientBundle, QueryType query) throws IOException {
            try {
                // simulate some latency
                Thread.currentThread().sleep(50)
            } catch (InterruptedException ignore) {
                // ignore
            }
            int count = counter.incrementAndGet()
            if (count > 3) {
                throw new IOException("Broken connection!")
            }
            List<ResultType> result = new ArrayList<>()
            for (int i = 0; i < 10; i++) {
                def resultValue = new ResultType(query.getId(), query.getId() + "-value-" + i)

                result.add(resultValue)
            }
            return result
        }
    }

    static class QueryType {
        String id

        QueryType() {
        }

        QueryType(String id) {
            this.id = id
        }

        boolean equals(o) {
            if (this.is(o)) return true
            if (getClass() != o.class) return false

            QueryType queryType = (QueryType) o

            if (id != queryType.id) return false

            return true
        }

        int hashCode() {
            return (id != null ? id.hashCode() : 0)
        }

        @Override
        String toString() {
            return "QueryType{" +
                "id='" + id + '\'' +
                '}'
        }
    }

    static class ResultType {
        String id
        String value

        ResultType() {
        }

        ResultType(String id, String value) {
            this.id = id
            this.value = value
        }

        boolean equals(o) {
            if (this.is(o)) return true
            if (getClass() != o.class) return false

            ResultType that = (ResultType) o

            if (id != that.id) return false
            if (value != that.value) return false

            return true
        }

        int hashCode() {
            int result
            result = (id != null ? id.hashCode() : 0)
            result = 31 * result + (value != null ? value.hashCode() : 0)
            return result
        }

        @Override
        String toString() {
            return "ResultType{" +
                "id='" + id + '\'' +
                ", value='" + value + '\'' +
                '}'
        }
    }
}
