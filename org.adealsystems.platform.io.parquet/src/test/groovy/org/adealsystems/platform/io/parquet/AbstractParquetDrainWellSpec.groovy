/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

package org.adealsystems.platform.io.parquet

import org.adealsystems.platform.io.DrainException
import org.adealsystems.platform.io.WellException
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.OutputFile
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.TempDir

class AbstractParquetDrainWellSpec extends Specification {
    @Shared
    @TempDir
    File temp

    @Shared
    Configuration configuration

    @Shared
    String tempURI

    @SuppressWarnings('unused')
    def setupSpec() {
        String tempPath = temp.getAbsolutePath()
        System.setProperty("hadoop.home.dir", tempPath)
        configuration = new Configuration()
        tempURI = temp.toURI().toURL().toString()
    }

    def "writing to drain and then reading from well works with codec #codec"() {
        given:
        Path path = new Path(tempURI, "foo")
        OutputFile outputFile = HadoopOutputFile.fromPath(path, configuration)
        AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.builder(outputFile)
        writerBuilder.withCompressionCodec(codec)
        EntryParquetDrain drain = new EntryParquetDrain(EntryAvroConstants.SCHEMA, writerBuilder)
        def entry1 = new Entry(1, "Value 1")
        def entry2 = new Entry(2, "Value 2")
        def entry3 = new Entry(3, "Value 3")

        when: 'entries are added'
        drain.add(entry1)
        drain.addAll([entry2, entry3])

        and: 'drain is closed'
        drain.close()
        and: 'we take those bytes and put them into a matching well'
        InputFile inputFile = HadoopInputFile.fromPath(path, configuration)
        AvroParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.builder(inputFile)

        // correct schema and codec are determined automatically from header
        EntryParquetWell well = new EntryParquetWell(readerBuilder)
        and: 'we read all entries from the well'
        List<Entry> entries = well.collect()
        and: 'the well is closed'
        well.close()

        then: 'the entries read from the well are the same we put into the drain'
        entries != null
        entries.size() == 3
        entries[0] == entry1
        entries[1] == entry2
        entries[2] == entry3

        where:
        // commented out codecs require additional dependencies,
        // incl. native libraries.
        codec << [
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.GZIP,
            //CompressionCodecName.LZO,
            //CompressionCodecName.BROTLI,
            //CompressionCodecName.LZ4,
            CompressionCodecName.ZSTD,
        ]
    }

    def 'add(..) throws exception if already closed'() {
        given:
        Path path = new Path(tempURI, "foo")
        OutputFile outputFile = HadoopOutputFile.fromPath(path, configuration)
        AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.builder(outputFile)
        EntryParquetDrain instance = new EntryParquetDrain(EntryAvroConstants.SCHEMA, writerBuilder)

        when:
        instance.close()
        and:
        instance.add(new Entry(1, "Value 1"))

        then:
        DrainException ex = thrown()
        ex.message == "Drain was already closed!"
    }

    def 'add(..) throws exception if value is invalid'() {
        given:
        Path path = new Path(tempURI, "foo")
        OutputFile outputFile = HadoopOutputFile.fromPath(path, configuration)
        AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.builder(outputFile)
        EntryParquetDrain instance = new EntryParquetDrain(EntryAvroConstants.SCHEMA, writerBuilder)

        when: 'we add an invalid value'
        instance.add(new Entry(1, null))

        then:
        DrainException ex = thrown()
        ex.message == "Exception while writing entry!"
        ex.cause.class == RuntimeException
    }

    def 'drain.getSchema() returns given Schema'() {
        given:
        Path path = new Path(tempURI, "foo")
        OutputFile outputFile = HadoopOutputFile.fromPath(path, configuration)
        AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.builder(outputFile)
        EntryParquetDrain instance = new EntryParquetDrain(EntryAvroConstants.SCHEMA, writerBuilder)

        expect:
        instance.getSchema() == EntryAvroConstants.SCHEMA
    }

    def 'closing drain twice is fine'() {
        given:
        Path path = new Path(tempURI, "foo")
        OutputFile outputFile = HadoopOutputFile.fromPath(path, configuration)
        AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.builder(outputFile)
        EntryParquetDrain instance = new EntryParquetDrain(EntryAvroConstants.SCHEMA, writerBuilder)

        when: 'closing drain twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def 'well works as expected in edge cases'() {
        given:
        Path path = new Path(tempURI, "foo")
        OutputFile outputFile = HadoopOutputFile.fromPath(path, configuration)
        AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.builder(outputFile)
        EntryParquetDrain drain = new EntryParquetDrain(EntryAvroConstants.SCHEMA, writerBuilder)
        def value1 = new Entry(1, "Value 1")
        drain.add(value1)
        drain.close()

        InputFile inputFile = HadoopInputFile.fromPath(path, configuration)
        AvroParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.builder(inputFile)

        // correct schema and codec are determined automatically from header
        EntryParquetWell instance = new EntryParquetWell(readerBuilder)

        expect:
        // no chance to retrieve Schema from reader that I'm aware of
        // instance.getSchema() == EntryAvroConstants.SCHEMA
        !instance.isConsumed()

        when:
        def iterator = instance.iterator()
        then:
        instance.isConsumed()

        when: 'we try to obtain iterator from consumed well'
        instance.iterator()
        then:
        WellException ex = thrown()
        ex.message == "A well can only be iterated once!"

        when:
        def value = iterator.next()
        then:
        value == value1

        when:
        iterator.next()
        then:
        thrown(NoSuchElementException)

        when: 'closing drain twice'
        instance.close()
        instance.close()

        then:
        noExceptionThrown()
    }

    def 'iterating a closed well fails as expected'() {
        given:
        Path path = new Path(tempURI, "foo")
        OutputFile outputFile = HadoopOutputFile.fromPath(path, configuration)
        AvroParquetWriter.Builder<GenericRecord> writerBuilder = AvroParquetWriter.builder(outputFile)
        EntryParquetDrain drain = new EntryParquetDrain(EntryAvroConstants.SCHEMA, writerBuilder)
        def value1 = new Entry(1, "Value 1")
        def value2 = new Entry(2, "Value 2")
        drain.addAll([value1, value2])
        drain.close()

        InputFile inputFile = HadoopInputFile.fromPath(path, configuration)
        AvroParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.builder(inputFile)

        // correct schema and codec are determined automatically from header
        EntryParquetWell instance = new EntryParquetWell(readerBuilder)
        def iterator = instance.iterator()

        expect:
        iterator.next() == value1

        when:
        instance.close()
        iterator.next()

        then:
        WellException ex = thrown()
        ex.message == "Well was already closed!"
    }

    // can't check exception while closing due to aRcHiTeCtUrE
}
