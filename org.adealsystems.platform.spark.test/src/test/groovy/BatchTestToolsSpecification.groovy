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

import org.adealsystems.platform.spark.test.BatchTestTools
import org.adealsystems.platform.spark.test.DatasetColumnDefinitions
import org.apache.spark.sql.SparkSession
import spock.lang.Shared
import spock.lang.Specification

class BatchTestToolsSpecification extends Specification {
    @Shared
    protected SparkSession spark

    def setupSpec() {
        println("Setup spec")
        spark = SparkSession.builder().master("local[*]").appName("TEST").getOrCreate()
    }

    def "createDataset works as expected"() {
        given:
        def simpleStruct = DatasetColumnDefinitions.builder()
            .string("a")
            .integer("b")
            .longint("c")
            .bool("d")
            .build()

        def defaultDateTimeStruct = DatasetColumnDefinitions.builder()
            .string("a")
            .date("b")
            .datetime("c")
            .build()

        def specificDateTimeStruct = DatasetColumnDefinitions.builder()
            .string("a")
            .date("b", "yyyy.MM.dd")
            .datetime("c", "yyMMdd:HHmmss")
            .build()

        when:
        def dataset1a = BatchTestTools.createDataset(
            spark,
            simpleStruct,
            "+---+--+-------+-----+\n" +
                "|  a| b|      c|    d|\n" +
                "+---+--+-------+-----+\n" +
                "|AAA|17|123456L| true|\n" +
                "|BBB|21|987654L|false|\n" +
                "|CCC|  |       |     |\n" +
                "+---+--+-------+-----+"
        )

        then:
        dataset1a != null
        dataset1a.show()

        when:
        def dataset1b = BatchTestTools.createDataset(
            spark,
            simpleStruct,
            BatchTestTools.RowFormat.create(),
            "AAA,17,123456, true",
            "BBB,21,987654,false",
            "CCC,  ,      ,     "
        )

        then:
        dataset1b != null
        dataset1b.show()

        when:
        def dataset1c = BatchTestTools.createDataset(
            spark,
            simpleStruct,
            BatchTestTools.RowFormat.create().withDelimiter(";"),
            "AAA;17;123456; true",
            "BBB;21;987654;false",
            "CCC;  ;      ;     "
        )

        then:
        dataset1c != null
        dataset1c.show()

        and:
        BatchTestTools.assertDatasetEqual(dataset1a, dataset1b)
        BatchTestTools.assertDatasetEqual(dataset1a, dataset1c)

        when:
        def dataset2 = BatchTestTools.createDataset(
            spark,
            simpleStruct,
            BatchTestTools.Rows.builder()
                .row("AAA", 17, 123456L, true)
                .row("BBB", 21, 987654L, false)
                .row("CCC", null, null, null)
                .build()
        )

        then:
        dataset2 != null
        dataset2.show()

        and:
        BatchTestTools.assertDatasetEqual(dataset1a, dataset2)

        when:
        def dataset3 = BatchTestTools.createDataset(
            spark,
            defaultDateTimeStruct,
            "+---+----------+----------------+\n" +
            "|  a|         b|               c|\n" +
            "+---+----------+----------------+\n" +
            "|AAA|2025-04-14|2025-04-14 07:28|\n" +
            "|BBB|2025-02-17|2025-02-17 17:19|\n" +
            "|CCC|          |                |\n" +
            "+---+----------+----------------+"
        )

        then:
        dataset3 != null
        dataset3.show()

        when:
        def dataset4 = BatchTestTools.createDataset(
            spark,
            specificDateTimeStruct,
            "+---+----------+-------------+\n" +
            "|  a|         b|            c|\n" +
            "+---+----------+-------------+\n" +
            "|AAA|2025.04.14|250414:072820|\n" +
            "|BBB|2025.02.17|250217:171905|\n" +
            "|CCC|          |             |\n" +
            "+---+----------+-------------+"
        )

        then:
        dataset4 != null
        dataset4.show()
    }

    def "valueAt works as expected"() {
        given:
        def simpleStruct = DatasetColumnDefinitions.builder()
            .string("a")
            .integer("b")
            .longint("c")
            .bool("d")
            .build()

        when:
        def dataset5 = BatchTestTools.createDataset(
            spark,
            simpleStruct,
            "+---+--+-------+-----+\n" +
            "|  a| b|      c|    d|\n" +
            "+---+--+-------+-----+\n" +
            "|AAA|17|123456L| true|\n" +
            "|BBB|21|987654L|false|\n" +
            "|CCC|  |       |     |\n" +
            "|AAA|27|1982376|false|\n" +
            "+---+--+-------+-----+"
        )

        then:
        dataset5 != null
        dataset5.show()

        and:
        BatchTestTools.valuesAt(dataset5, "b[a='BBB']", Integer.class) == [21] as Set
        BatchTestTools.valuesAt(dataset5, "b[a='AAA']", Integer.class) == [17, 27] as Set
        BatchTestTools.valuesAt(dataset5, "b[a='AAA' & d=false]", Integer.class) == [27] as Set
        BatchTestTools.valuesAt(dataset5, "a[d=false]", String.class) == ['AAA', 'BBB'] as Set
    }
}
