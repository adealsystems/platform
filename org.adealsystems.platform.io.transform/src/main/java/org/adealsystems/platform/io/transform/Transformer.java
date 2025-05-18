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

package org.adealsystems.platform.io.transform;

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.DrainFactory;
import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.WellFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;

/**
 * Reads entries from inputs and merges them into a single output.
 * <p>
 * This class is designed to be robust.
 * <p>
 * Any errors are simply logged and registered in the returned metrics.
 * Use <code>TransformerMetrics.hasErrors()</code> to conveniently check for any error
 * or dive deeper into metrics if you need more flexible error handling.
 *
 * @param <I> the type of the parameter given to the <code>WellFactory</code>
 * @param <J> the type of the <code>Well</code> created by the <code>WellFactory</code>
 * @param <O> the type of the parameter given to the <code>DrainFactory</code>
 * @param <P> the type of the <code>Drain</code> created by the <code>DrainFactory</code>
 * @see TransformerMetrics#hasErrors() Use <code>TransformerMetrics.hasErrors()</code> to conveniently check for any error
 */
public class Transformer<I, J, O, P> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Transformer.class);
    private static final String INPUT_MDC_KEY = "TransformerInput";

    private final WellFactory<I, J> wellFactory;
    private final DrainFactory<O, P> drainFactory;
    private final Function<J, Iterable<P>> convertFunction;

    /**
     * Create instance with the given parameters.
     * <p>
     * Input entries are retrieved from <code>Well</code> instances created with the given <code>WellFactory</code>.
     * <p>
     * Output entries are written to a <code>Drain</code> created with the given <code>DrainFactory</code>.
     * <p>
     * Conversion is performed by the given <code>convertFunction</code>.
     * Returning <code>null</code> or an empty <code>Collection</code> from the <code>convertFunction</code> will
     * not cause any problems but simply skip the input entry. This can be used to filter input entries.
     * <p>
     * If neither conversion nor filtering is necessary, you can use <code>Transformer::identityFunction</code> as the
     * <code>convertFunction</code>.
     *
     * @param wellFactory     used to create <code>Well</code> instances for given inputs
     * @param drainFactory    used to create <code>Drain</code> instance for given output
     * @param convertFunction used to convert or filter input entries
     * @throws NullPointerException if any parameter is null
     * @see Transformer#identityFunction <code>Transformer::identityFunction</code> if neither conversion nor filtering is required
     */
    public Transformer(WellFactory<I, J> wellFactory, DrainFactory<O, P> drainFactory, Function<J, Iterable<P>> convertFunction) {
        this.wellFactory = Objects.requireNonNull(wellFactory, "wellFactory must not be null!");
        this.drainFactory = Objects.requireNonNull(drainFactory, "drainFactory must not be null!");
        this.convertFunction = Objects.requireNonNull(convertFunction, "convertFunction must not be null!");
    }

    /**
     * Reads entries from the given input and writes them into the output.
     *
     * @param input  the input that will by read
     * @param output the output that input entries will be written into
     * @return metrics of the transformation
     * @throws NullPointerException if any parameter is <code>null</code> or <code>DrainFactory</code>
     *                              returns <code>null</code> for the given <code>output</code>
     * @throws RuntimeException     if <code>DrainFactory</code> throws <code>RuntimeException</code> for the given
     *                              <code>output</code>
     */
    public TransformerMetrics transform(I input, O output) {
        Objects.requireNonNull(input, "input must not be null!");
        Objects.requireNonNull(output, "output must not be null!");

        return transform(Collections.singleton(input), output);
    }

    /**
     * Reads entries from the given inputs and merges them into the output.
     *
     * @param inputs the inputs that will by read
     * @param output the output that input entries will be merged into
     * @return metrics of the transformation
     * @throws NullPointerException if any parameter is <code>null</code> or <code>DrainFactory</code>
     *                              returns <code>null</code> for the given <code>output</code>
     * @throws RuntimeException     if <code>DrainFactory</code> throws <code>RuntimeException</code> for the given
     *                              <code>output</code>
     */
    public TransformerMetrics transform(Iterable<I> inputs, O output) {
        Objects.requireNonNull(inputs, "inputs must not be null!");
        Objects.requireNonNull(output, "output must not be null!");

        TransformerMetrics metrics = new TransformerMetrics();

        try (
            Drain<P> drain = drainFactory.createDrain(output)
        ) {
            Objects.requireNonNull(drain, "drainFactory returned null for \"" + output + "\"!");
            processInputs(inputs, drain, metrics);
        }

        return metrics;
    }

    /**
     * This method can be used as <code>convertFunction</code> if neither conversion nor filtering is required.
     * <p>
     * It simply returns <code>null</code> if <code>input</code> was null or a <code>List</code>
     * containing <code>input</code> otherwise.
     *
     * @param input the input
     * @param <E>   the type of both input parameter and return type
     * @return <code>null</code> if <code>input</code> was null or a <code>List</code>
     * containing <code>input</code> otherwise.
     */
    public static <E> Iterable<E> identityFunction(E input) {
        if (input == null) {
            return null;
        }
        return Collections.singletonList(input);
    }

    private void processInputs(Iterable<I> inputs, Drain<P> drain, TransformerMetrics metrics) {
        try {
            for (I input : inputs) {
                if (!processInput(input, drain, metrics)) {
                    // false indicates a write error
                    break;
                }
            }
        } catch (Throwable t) {
            LOGGER.warn("Exception while iterating inputs!", t);
            metrics.addInputIterationError();
        }
    }

    private boolean processInput(I input, Drain<P> drain, TransformerMetrics metrics) {
        metrics.addTotalInput();

        if (input == null) {
            LOGGER.warn("inputs should not contain null! Skipping it...");
            metrics.addSkippedInput();
            return true;
        }

        String inputString = input.toString();
        try {
            MDC.put(INPUT_MDC_KEY, inputString);
            try (Well<J> well = wellFactory.createWell(input)) {
                return processWell(inputString, well, drain, metrics);
            } catch (Throwable t) {
                LOGGER.warn("Exception while creating well for {}!", inputString, t);
                metrics.addInputError();
            }
            return true;
        } finally {
            MDC.remove(INPUT_MDC_KEY);
        }
    }

    private boolean processWell(String inputString, Well<J> well, Drain<P> drain, TransformerMetrics metrics) {
        if (well == null) {
            metrics.addSkippedInput();
            return true;
        }

        LOGGER.info("Processing {}", inputString);
        try {
            for (J entry : well) {
                if (!processEntry(entry, drain, metrics)) {
                    return false;
                }
            }
            metrics.addTransformedInput();
        } catch (Throwable t) {
            LOGGER.warn("Exception while retrieving entry from {}!", inputString, t);
            metrics.addReadError();
        }
        return true;
    }

    private boolean processEntry(J entry, Drain<P> drain, TransformerMetrics metrics) {
        metrics.addReadEntry();

        Iterable<P> outputEntries = convert(entry, metrics);
        if (outputEntries == null) {
            LOGGER.debug("Skipping {}...", entry);
            metrics.addSkippedInputEntry();
            return true;
        }

        boolean empty = true;
        try {
            for (P outputEntry : outputEntries) {
                empty = false;
                if (!writeOutputEntry(outputEntry, drain, metrics)) {
                    return false;
                }
            }
        } catch (Throwable t) {
            LOGGER.warn("Exception while iterating outputs!", t);
            metrics.addOutputIterationError();
        }

        if (empty) {
            LOGGER.debug("Skipping {}...", entry);
            metrics.addSkippedInputEntry();
        }

        return true;
    }

    private boolean writeOutputEntry(P outputEntry, Drain<P> drain, TransformerMetrics metrics) {
        if (outputEntry == null) {
            metrics.addSkippedOutputEntry();
            return true;
        }

        try {
            drain.add(outputEntry);
            metrics.addWrittenEntry();
            return true;
        } catch (Throwable t) {
            LOGGER.warn("Exception while writing {}!", outputEntry, t);
            metrics.addWriteError();
            return false;
        }
    }

    @SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
    private Iterable<P> convert(J input, TransformerMetrics metrics) {
        if (input == null) {
            return null;
        }

        try {
            return convertFunction.apply(input);
        } catch (Throwable t) {
            LOGGER.warn("Exception while converting {}!", input, t);
            metrics.addConversionError();
        }

        return null;
    }
}
