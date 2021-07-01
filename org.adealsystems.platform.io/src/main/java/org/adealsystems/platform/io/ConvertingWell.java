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

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

public class ConvertingWell<I, O> implements Well<O> {

    private final Well<I> innerWell;
    private final Function<? super I, O> convertFunction;

    public ConvertingWell(Well<I> innerWell, Function<? super I, O> convertFunction) {
        this.innerWell = Objects.requireNonNull(innerWell, "innerWell must not be null!");
        this.convertFunction = Objects.requireNonNull(convertFunction, "convertFunction must not be null!");
    }

    @Override
    public boolean isConsumed() {
        return innerWell.isConsumed();
    }

    @Override
    public void close() {
        innerWell.close();
    }

    @Override
    public Iterator<O> iterator() {
        return new InternalIterator();
    }

    private class InternalIterator implements Iterator<O> {
        private final Iterator<I> iterator;

        InternalIterator() {
            iterator = innerWell.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public O next() {
            return convertFunction.apply(iterator.next());
        }
    }
}
