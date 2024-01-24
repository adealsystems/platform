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

package org.adealsystems.platform.io;

import java.util.Objects;

public class CountingDrain<E> implements Drain<E> {
    private Drain<E> innerDrain;
    private int counter;

    public CountingDrain(Drain<E> innerDrain) {
        this.innerDrain = Objects.requireNonNull(innerDrain, "innerDrain must not be null!");
    }

    public int getCounter() {
        return counter;
    }

    @Override
    public void add(E entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        if (innerDrain == null) {
            throw new DrainException("Drain was already closed!");
        }
        innerDrain.add(entry);
        counter++;
    }

    @Override
    public void addAll(Iterable<E> entries) {
        Objects.requireNonNull(entries, "entries must not be null!");
        for (E entry : entries) {
            add(Objects.requireNonNull(entry, "entries must not contain null!"));
        }
    }

    @Override
    public void close() {
        if(innerDrain == null) {
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
