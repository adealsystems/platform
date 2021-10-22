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

package org.adealsystems.platform.io.transform;

import java.util.Objects;

/**
 * The metrics returned by a Transformer.
 *
 * <dl>
 *     <dt>readEntries</dt>
 *     <dd>The total number of entries read from all inputs.</dd>
 *
 *     <dt>writtenEntries</dt>
 *     <dd>The total number of entries written to the output.</dd>
 *
 *     <dt>skippedEntries</dt>
 *     <dd>The total number of skipped entries.
 * <p>
 *     Entries are skipped if the <code>convertFunction</code> returns
 *     <code>null</code> or throws an exception during conversion. <code>writtenEntries</code> plus
 *     <code>skippedEntries</code> should always be equal to <code>readEntries</code>.</dd>
 *
 *     <dt>totalInputs</dt>
 *     <dd>The total number of processed inputs.</dd>
 *
 *     <dt>transformedInputs</dt>
 *     <dd>The number of successfully transformed inputs.</dd>
 *
 *     <dt>skippedInputs</dt>
 *     <dd>The number of skipped inputs.
 * <p>
 *     An input is skipped if it's either directly <code>null</code>
 *     in the <code>inputs</code> <code>Iterable</code> or the <code>WellFactory</code>
 *     returns <code>null</code>.</dd>
 *
 *     <dt>conversionErrors</dt>
 *     <dd>The number of conversion errors, i.e. the number of times the <code>conversionFunction</code>
 *     has thrown an exception.
 * <p>
 *     This should not happen and would indicate a bug in your <code>conversionFunction</code>.</dd>
 *
 *     <dt>readErrors</dt>
 *     <dd>The number of read errors, i.e. the number of times reading from a <code>Well</code>
 *     has thrown an exception. Processing of any given input stops after an exception so this
 *     number will never be higher than <code>totalInputs</code>.</dd>
 *
 *     <dt>writeErrors</dt>
 *     <dd>The number of write errors.
 * <p>
 *     Transformation stops after a write error so this number will be at most <code>1</code> for a
 *     single <code>transform</code> call.
 * <p>
 *     This is not a <code>boolean</code> because multiple <code>TransformerMetrics</code> can be
 *     added for easier handling.</dd>
 *
 *     <dt>inputErrors</dt>
 *     <dd>The number of input errors, i.e. exceptions thrown while creating a <code>Well</code> for a
 *     given <code>input</code>.
 * <p>
 *     This probably indicates a bug in your <code>WellFactory</code>. You could instead
 *     log the exception and return null to skip the input.</dd>
 *
 *     <dt>inputIterationErrors</dt>
 *     <dd>The number of input iteration errors, i.e. exceptions thrown while iterating over the <code>inputs</code>
 *     <code>Iterable</code>. <code>ConcurrentModificationException</code> would be one example.
 * <p>
 *     This should not happen and would indicate a bug in your code.</dd>
 * </dl>
 */
public class TransformerMetrics {
    private int readEntries;
    private int writtenEntries;
    private int skippedEntries;
    private int totalInputs;
    private int transformedInputs;
    private int skippedInputs;
    private int conversionErrors;
    private int readErrors;
    private int writeErrors;
    private int inputErrors;
    private int inputIterationErrors;

    public int getReadEntries() {
        return readEntries;
    }

    public void setReadEntries(int readEntries) {
        this.readEntries = readEntries;
    }

    public int getWrittenEntries() {
        return writtenEntries;
    }

    public void setWrittenEntries(int writtenEntries) {
        this.writtenEntries = writtenEntries;
    }

    public int getSkippedEntries() {
        return skippedEntries;
    }

    public void setSkippedEntries(int skippedEntries) {
        this.skippedEntries = skippedEntries;
    }

    public int getTotalInputs() {
        return totalInputs;
    }

    public void setTotalInputs(int totalInputs) {
        this.totalInputs = totalInputs;
    }

    public int getTransformedInputs() {
        return transformedInputs;
    }

    public void setTransformedInputs(int transformedInputs) {
        this.transformedInputs = transformedInputs;
    }

    public int getSkippedInputs() {
        return skippedInputs;
    }

    public void setSkippedInputs(int skippedInputs) {
        this.skippedInputs = skippedInputs;
    }

    public int getConversionErrors() {
        return conversionErrors;
    }

    public void setConversionErrors(int conversionErrors) {
        this.conversionErrors = conversionErrors;
    }

    public int getReadErrors() {
        return readErrors;
    }

    public void setReadErrors(int readErrors) {
        this.readErrors = readErrors;
    }

    public int getWriteErrors() {
        return writeErrors;
    }

    public void setWriteErrors(int writeErrors) {
        this.writeErrors = writeErrors;
    }

    public int getInputErrors() {
        return inputErrors;
    }

    public void setInputErrors(int inputErrors) {
        this.inputErrors = inputErrors;
    }

    public int getInputIterationErrors() {
        return inputIterationErrors;
    }

    public void setInputIterationErrors(int inputIterationErrors) {
        this.inputIterationErrors = inputIterationErrors;
    }

    public void addReadEntry() {
        readEntries++;
    }

    public void addWrittenEntry() {
        writtenEntries++;
    }

    public void addSkippedEntry() {
        skippedEntries++;
    }

    public void addTotalInput() {
        totalInputs++;
    }

    public void addTransformedInput() {
        transformedInputs++;
    }

    public void addSkippedInput() {
        skippedInputs++;
    }

    public void addConversionError() {
        conversionErrors++;
    }

    public void addReadError() {
        readErrors++;
    }

    public void addWriteError() {
        writeErrors++;
    }

    public void addInputError() {
        inputErrors++;
    }

    public void addInputIterationError() {
        inputIterationErrors++;
    }

    /**
     * Adds the given metrics to this metrics.
     *
     * @param metrics the metrics to add to our metrics
     * @throws NullPointerException if metrics is null
     */
    public void add(TransformerMetrics metrics) {
        Objects.requireNonNull(metrics, "metrics must not be null!");

        this.readEntries += metrics.readEntries;
        this.writtenEntries += metrics.writtenEntries;
        this.skippedEntries += metrics.skippedEntries;
        this.totalInputs += metrics.totalInputs;
        this.transformedInputs += metrics.transformedInputs;
        this.skippedInputs += metrics.skippedInputs;
        this.conversionErrors += metrics.conversionErrors;
        this.readErrors += metrics.readErrors;
        this.writeErrors += metrics.writeErrors;
        this.inputErrors += metrics.inputErrors;
        this.inputIterationErrors += metrics.inputIterationErrors;
    }

    public boolean hasErrors() {
        return conversionErrors != 0 || readErrors != 0 || writeErrors != 0 || inputErrors != 0 || inputIterationErrors != 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransformerMetrics that = (TransformerMetrics) o;
        return readEntries == that.readEntries && writtenEntries == that.writtenEntries && skippedEntries == that.skippedEntries && totalInputs == that.totalInputs && transformedInputs == that.transformedInputs && skippedInputs == that.skippedInputs && conversionErrors == that.conversionErrors && readErrors == that.readErrors && writeErrors == that.writeErrors && inputErrors == that.inputErrors && inputIterationErrors == that.inputIterationErrors;
    }

    @Override
    public int hashCode() {
        return Objects.hash(readEntries, writtenEntries, skippedEntries, totalInputs, transformedInputs, skippedInputs, conversionErrors, readErrors, writeErrors, inputErrors, inputIterationErrors);
    }

    @Override
    public String toString() {
        return "TransformerMetrics{" +
            "readEntries=" + readEntries +
            ", writtenEntries=" + writtenEntries +
            ", skippedEntries=" + skippedEntries +
            ", totalInputs=" + totalInputs +
            ", transformedInputs=" + transformedInputs +
            ", skippedInputs=" + skippedInputs +
            ", conversionErrors=" + conversionErrors +
            ", readErrors=" + readErrors +
            ", writeErrors=" + writeErrors +
            ", inputErrors=" + inputErrors +
            ", inputIterationErrors=" + inputIterationErrors +
            '}';
    }
}
