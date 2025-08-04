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

package org.adealsystems.platform.spark

import org.adealsystems.platform.spark.test.BatchTestTools
import org.adealsystems.platform.spark.test.DatasetColumnDefinitions
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import spock.lang.Shared
import spock.lang.Specification

import java.util.function.BiConsumer

class DatasetLoggerSpec extends Specification {

    @Shared
    protected SparkSession sparkSession

    // this works, by naming convention, like @BeforeClass
    def setupSpec() {
        println("Setup Spec")
        sparkSession = SparkSession.builder().master("local[*]").appName("TEST").getOrCreate()
    }

    def 'Single session works as expected'() {
        given:
        DatasetLogger dsl = new DatasetLogger(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()))
        dsl.addSessionAnalyser('session-a', new DummySessionAnalyser())

        def dataset1 = BatchTestTools.createDataset(
            sparkSession,
            DatasetColumnDefinitions.builder()
            .integer('id')
            .string('key-1')
            .string('key-2')
            .build(),
            BatchTestTools.RowFormat.create().withDelimiter(";"),
            '1;aaa-1;aaa-2',
            '2;aaa-2;aaa-2',
            '3;aaa-3;aaa-3',
            '4;aaa-4;aaa-4',
            '5;aaa-5;aaa-5'
        )

        when:
        dsl.startSession('session-a')
        dsl.analyse('analysis-1', dataset1)

        then:
        dsl.getActiveSessions() == ['session-a'] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == ['analysis-1': dataset1]

        when:
        def dataset2 = dataset1.filter(dataset1.col('id').gt(2))
        dsl.analyse('analysis-2', dataset2)

        then:
        dsl.getActiveSessions() == ['session-a'] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == ['analysis-1': dataset1, 'analysis-2': dataset2]

        when:
        dsl.stopSession('session-a')

        then:
        dsl.getActiveSessions() == [] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == [:]
    }

    def 'Multiple sessions sequentially work as expected'() {
        given:
        DatasetLogger dsl = new DatasetLogger(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()))
        dsl.addSessionAnalyser('session-a', new DummySessionAnalyser(id: 'analyser-A'))
        dsl.addSessionAnalyser('session-b', new DummySessionAnalyser(id: 'analyser-B'))

        def dataset1 = BatchTestTools.createDataset(
            sparkSession,
            DatasetColumnDefinitions.builder()
                .integer('id')
                .string('key-1')
                .string('key-2')
                .build(),
            BatchTestTools.RowFormat.create().withDelimiter(";"),
            '1;aaa-1;aaa-2',
            '2;aaa-2;aaa-2',
            '3;aaa-3;aaa-3',
            '4;aaa-4;aaa-4',
            '5;aaa-5;aaa-5'
        )

        when:
        dsl.startSession('session-a')
        dsl.analyse('analysis-1', dataset1)

        then:
        dsl.getActiveSessions() == ['session-a'] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == ['analysis-1': dataset1]
        dsl.getAnalyserSessions().get('session-b') == null

        when:
        dsl.stopSession('session-a')
        dsl.startSession('session-b')
        def dataset2 = dataset1.filter(dataset1.col('id').gt(2))
        dsl.analyse('analysis-2', dataset2)

        then:
        dsl.getActiveSessions() == ['session-b'] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == [:]
        dsl.getAnalyserSessions().get('session-b').datasets == ['analysis-2': dataset2]

        when:
        dsl.stopSession('session-b')

        then:
        dsl.getActiveSessions() == [] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == [:]
        dsl.getAnalyserSessions().get('session-b').datasets == [:]
    }

    def 'Multiple sessions in parallel work as expected'() {
        given:
        DatasetLogger dsl = new DatasetLogger(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()))
        dsl.addSessionAnalyser('session-a', new DummySessionAnalyser(id: 'analyser-A'))
        dsl.addSessionAnalyser('session-b', new DummySessionAnalyser(id: 'analyser-B'))

        def dataset1 = BatchTestTools.createDataset(
            sparkSession,
            DatasetColumnDefinitions.builder()
                .integer('id')
                .string('key-1')
                .string('key-2')
                .build(),
            BatchTestTools.RowFormat.create().withDelimiter(";"),
            '1;aaa-1;aaa-2',
            '2;aaa-2;aaa-2',
            '3;aaa-3;aaa-3',
            '4;aaa-4;aaa-4',
            '5;aaa-5;aaa-5'
        )

        when:
        dsl.startSession('session-a')
        dsl.analyse('analysis-1', dataset1)

        then:
        dsl.getActiveSessions() == ['session-a'] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == ['analysis-1': dataset1]
        dsl.getAnalyserSessions().get('session-b') == null

        when:
        dsl.startSession('session-b')
        def dataset2 = dataset1.filter(dataset1.col('id').gt(2))
        dsl.analyse('analysis-2', dataset2)

        then:
        dsl.getActiveSessions() == ['session-a', 'session-b'] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == ['analysis-1': dataset1, 'analysis-2': dataset2]
        dsl.getAnalyserSessions().get('session-b').datasets == ['analysis-2': dataset2]

        when:
        dsl.stopSession('session-b')

        then:
        dsl.getActiveSessions() == ['session-a'] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == ['analysis-1': dataset1, 'analysis-2': dataset2]
        dsl.getAnalyserSessions().get('session-b').datasets == [:]

        when:
        dsl.stopSession('session-a')

        then:
        dsl.getActiveSessions() == [] as Set
        dsl.getAnalyserSessions().get('session-a').datasets == [:]
        dsl.getAnalyserSessions().get('session-b').datasets == [:]
    }

    static class DummySessionAnalyser implements BiConsumer<DatasetLogger.AnalyserSession, String> {
        String id = 'default'

        @Override
        void accept(DatasetLogger.AnalyserSession session, String analysis) {
            println('[Analyser ' + id + '] Accepting session ' + session.code + ' and analysis ' + analysis)
        }
    }
}
