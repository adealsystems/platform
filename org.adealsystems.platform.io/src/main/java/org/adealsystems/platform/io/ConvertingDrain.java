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

package org.adealsystems.platform.io;

import java.util.Objects;
import java.util.function.Function;

public class ConvertingDrain<I, O> implements Drain<I> {

    private final Drain<O> innerDrain;
    private final Function<? super I, O> convertFunction;

    public ConvertingDrain(Drain<O> innerDrain, Function<? super I, O> convertFunction) {
        this.innerDrain = Objects.requireNonNull(innerDrain, "innerDrain must not be null!");
        this.convertFunction = Objects.requireNonNull(convertFunction, "convertFunction must not be null!");
    }

    @Override
    public void add(I entry) {
        Objects.requireNonNull(entry, "entry must not be null!");

        innerDrain.add(convertFunction.apply(entry));
    }

    @Override
    public void addAll(Iterable<I> entries) {
        Objects.requireNonNull(entries, "entries must not be null!");
        for (I entry : entries) {
            innerDrain.add(convertFunction.apply(Objects.requireNonNull(entry, "entries must not contain null!")));
        }
    }

    @Override
    public void close() {
        innerDrain.close();
    }
}
