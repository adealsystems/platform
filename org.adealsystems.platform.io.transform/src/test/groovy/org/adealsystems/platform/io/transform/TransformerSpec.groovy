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

package org.adealsystems.platform.io.transform

import org.adealsystems.platform.io.transform.test.BrokenDataObjectList
import org.adealsystems.platform.io.transform.test.BrokenStringIterable
import org.adealsystems.platform.io.transform.test.DataObject
import org.adealsystems.platform.io.transform.test.DataObjectDrainFactory
import org.adealsystems.platform.io.transform.test.DataObjectWellFactory
import spock.lang.Specification

class TransformerSpec extends Specification {

    def "happy path works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        def metrics = instance.transform(["foo", "bar"], "output")

        then:
        metrics != null
        !metrics.hasErrors()
        metrics.readEntries == 20
        metrics.writtenEntries == 20
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 2
        metrics.transformedInputs == 2
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 20
        drainList.contains(new DataObject(7, "foo 7"))
        drainList.contains(new DataObject(8, "foo 8"))
    }

    def "happy path works as expected with single input"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        def metrics = instance.transform("foo", "output")

        then:
        metrics != null
        !metrics.hasErrors()
        metrics.readEntries == 10
        metrics.writtenEntries == 10
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 1
        metrics.transformedInputs == 1
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 10
        drainList.contains(new DataObject(7, "foo 7"))
        drainList.contains(new DataObject(8, "foo 8"))
    }

    def "skipping works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, TransformerSpec::skippingConversion)

        when:
        def metrics = instance.transform(["foo", "bar"], "output")

        then:
        metrics != null
        !metrics.hasErrors()
        metrics.readEntries == 20
        metrics.writtenEntries == 10
        metrics.skippedInputEntries == 10
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 2
        metrics.transformedInputs == 2
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 10
        drainList.contains(new DataObject(7, "foo 7"))
        !drainList.contains(new DataObject(8, "foo 8"))
    }

    def "alternative skipping works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, TransformerSpec::alternativeSkippingConversion)

        when:
        def metrics = instance.transform(["foo", "bar"], "output")

        then:
        metrics != null
        !metrics.hasErrors()
        metrics.readEntries == 20
        metrics.writtenEntries == 10
        metrics.skippedInputEntries == 10
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 2
        metrics.transformedInputs == 2
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 10
        drainList.contains(new DataObject(7, "foo 7"))
        !drainList.contains(new DataObject(8, "foo 8"))
    }

    def "double with null works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, TransformerSpec::doubleWithNullConversion)

        when:
        def metrics = instance.transform(["foo", "bar"], "output")

        then:
        metrics != null
        !metrics.hasErrors()
        metrics.readEntries == 20
        metrics.writtenEntries == 40
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 20
        metrics.totalInputs == 2
        metrics.transformedInputs == 2
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 40
        drainList.contains(new DataObject(7, "foo 7"))
        drainList.contains(new DataObject(8, "foo 8"))
    }

    def "broken output collection works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, TransformerSpec::brokenCollectionConversion)

        when:
        def metrics = instance.transform(["foo", "bar"], "output")

        then:
        metrics != null
        metrics.hasErrors()
        metrics.readEntries == 20
        metrics.writtenEntries == 20
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 2
        metrics.transformedInputs == 2
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 20

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 20
        drainList.contains(new DataObject(7, "foo 7"))
        drainList.contains(new DataObject(8, "foo 8"))
    }

    def "failing conversion works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, TransformerSpec::failingConversion)

        when:
        def metrics = instance.transform(["foo", "bar"], "output")

        then:
        metrics != null
        metrics.hasErrors()
        metrics.readEntries == 20
        metrics.writtenEntries == 10
        metrics.skippedInputEntries == 10 // conversion error is also counted as skipped
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 2
        metrics.transformedInputs == 2
        metrics.skippedInputs == 0
        metrics.conversionErrors == 10
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 10
        drainList.contains(new DataObject(7, "foo 7"))
        !drainList.contains(new DataObject(8, "foo 8"))
    }

    def "skipping inputs works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        def metrics = instance.transform(["foo", "null", "bar", null], "output")

        then:
        metrics != null
        !metrics.hasErrors()
        metrics.readEntries == 20
        metrics.writtenEntries == 20
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 4
        metrics.transformedInputs == 2
        metrics.skippedInputs == 2
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 20
        drainList.contains(new DataObject(7, "foo 7"))
        drainList.contains(new DataObject(8, "foo 8"))
    }

    def "null input entries work as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        def metrics = instance.transform(["foo", "nullEntries", "bar"], "output")

        then:
        metrics != null
        !metrics.hasErrors()
        metrics.readEntries == 30
        metrics.writtenEntries == 25
        metrics.skippedInputEntries == 5
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 3
        metrics.transformedInputs == 3
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 25
        drainList.contains(new DataObject(7, "foo 7"))
        drainList.contains(new DataObject(1, "nullEntries 1"))
        drainList.contains(new DataObject(8, "bar 8"))
    }

    def "exception while writing work as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        def metrics = instance.transform("foo", "broken")

        then:
        metrics != null
        metrics.hasErrors()
        metrics.readEntries == 6
        metrics.writtenEntries == 5
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 1
        metrics.transformedInputs == 0
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 1
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getBrokenDrainInstance().content
        drainList.size() == 5
        drainList.contains(new DataObject(3, "foo 3"))
    }

    def "exception while reading work as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        def metrics = instance.transform(["foo", "broken", "bar", "broken"], "output")

        then:
        metrics != null
        metrics.hasErrors()
        metrics.readEntries == 30
        metrics.writtenEntries == 30
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 4
        metrics.transformedInputs == 2
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 2
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 30
        drainList.contains(new DataObject(3, "foo 3"))
        drainList.contains(new DataObject(5, "broken 5"))
        drainList.contains(new DataObject(3, "bar 3"))
    }

    def "exception while iterating inputs works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        def metrics = instance.transform(new BrokenStringIterable(), "output")

        then:
        metrics != null
        metrics.hasErrors()
        metrics.readEntries == 10
        metrics.writtenEntries == 10
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 1
        metrics.transformedInputs == 1
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 0
        metrics.inputIterationErrors == 1
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 10
        drainList.contains(new DataObject(3, "input 3"))
    }

    def "handling of failing inputs works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        def metrics = instance.transform(["foo", "fail", "bar", "fail"], "output")

        then:
        metrics != null
        metrics.hasErrors()
        metrics.readEntries == 20
        metrics.writtenEntries == 20
        metrics.skippedInputEntries == 0
        metrics.skippedOutputEntries == 0
        metrics.totalInputs == 4
        metrics.transformedInputs == 2
        metrics.skippedInputs == 0
        metrics.conversionErrors == 0
        metrics.readErrors == 0
        metrics.writeErrors == 0
        metrics.inputErrors == 2
        metrics.inputIterationErrors == 0
        metrics.outputIterationErrors == 0

        def drainList = drainFactory.getDrainInstance().content
        drainList.size() == 20
        drainList.contains(new DataObject(7, "foo 7"))
        drainList.contains(new DataObject(8, "foo 8"))
    }

    def "failing output works as expected"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        instance.transform(["foo", "bar"], "fail")

        then:
        IllegalStateException ex = thrown()
        ex.message == "Failed to create Drain because this is a test!"
    }

    def "constructors throw expected exceptions"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()

        when:
        new Transformer<>(null, drainFactory, Transformer::identityFunction)
        then:
        NullPointerException ex = thrown()
        ex.message == "wellFactory must not be null!"

        when:
        new Transformer<>(wellFactory, null, Transformer::identityFunction)
        then:
        ex = thrown()
        ex.message == "drainFactory must not be null!"

        when:
        new Transformer<>(wellFactory, drainFactory, null)
        then:
        ex = thrown()
        ex.message == "convertFunction must not be null!"
    }

    def "transform calls throw expected exceptions"() {
        given:
        DataObjectDrainFactory drainFactory = new DataObjectDrainFactory()
        DataObjectWellFactory wellFactory = new DataObjectWellFactory()
        Transformer<String, DataObject, String, DataObject> instance =
            new Transformer(wellFactory, drainFactory, Transformer::identityFunction)

        when:
        instance.transform((String) null, "output")
        then:
        NullPointerException ex = thrown()
        ex.message == "input must not be null!"

        when:
        instance.transform((Iterable<String>) null, "output")
        then:
        ex = thrown()
        ex.message == "inputs must not be null!"

        when:
        instance.transform("input", null)
        then:
        ex = thrown()
        ex.message == "output must not be null!"

        when:
        instance.transform(["input1", "input2"], null)
        then:
        ex = thrown()
        ex.message == "output must not be null!"

        when:
        instance.transform(["input1", "input2"], "null")
        then:
        ex = thrown()
        ex.message == "drainFactory returned null for \"null\"!"
    }

    def "identity function works as expected"() {
        expect:
        Transformer.identityFunction(null) == null
        Transformer.identityFunction("foo") == ["foo"]
    }

    static Collection<DataObject> skippingConversion(DataObject input) {
        if (input == null || input.id % 2 == 0) {
            return null
        }
        return Collections.singletonList(input)
    }

    static Collection<DataObject> alternativeSkippingConversion(DataObject input) {
        if (input == null || input.id % 2 == 0) {
            return Collections.emptyList()
        }
        return Collections.singletonList(input)
    }

    static Collection<DataObject> doubleWithNullConversion(DataObject input) {
        return Arrays.asList(input, null, input)
    }

    static Collection<DataObject> brokenCollectionConversion(DataObject input) {
        return new BrokenDataObjectList(Collections.singletonList(input))
    }

    static Collection<DataObject> failingConversion(DataObject input) {
        if (input == null) {
            return null
        }
        if (input.id % 2 == 0) {
            throw new IllegalStateException("Conversion failed because this is a test!")
        }
        return Collections.singletonList(input)
    }
}
