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

package org.adealsystems.platform.io;

import java.util.Objects;
import java.util.function.Function;

public class ConvertingDrain<I, O> implements Drain<I> {

    private Drain<O> innerDrain;
    private final Function<? super I, O> convertFunction;

    public ConvertingDrain(Drain<O> innerDrain, Function<? super I, O> convertFunction) {
        this.innerDrain = Objects.requireNonNull(innerDrain, "innerDrain must not be null!");
        this.convertFunction = Objects.requireNonNull(convertFunction, "convertFunction must not be null!");
    }

    @Override
    public void add(I entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        if (innerDrain == null) {
            throw new DrainException("Drain was already closed!");
        }
        O result;
        try {
            result = convertFunction.apply(entry);
        } catch (Throwable t) {
            throw new DrainException("Exception while converting entry " + entry + "!", t);
        }
        innerDrain.add(result);
    }

    @Override
    public void addAll(Iterable<I> entries) {
        Objects.requireNonNull(entries, "entries must not be null!");
        for (I entry : entries) {
            add(Objects.requireNonNull(entry, "entries must not contain null!"));
        }
    }

    @Override
    public void close() {
        if (innerDrain == null) {
            return;
        }
        Throwable throwable = null;
        try {
            innerDrain.close();
        } catch (Throwable t) {
            throwable = t;
        }
        innerDrain = null;
        if (throwable != null) {
            throw new DrainException("Exception while closing innerDrain!", throwable);
        }
    }
}
