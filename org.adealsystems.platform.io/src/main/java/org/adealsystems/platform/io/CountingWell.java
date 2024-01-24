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

import java.util.Iterator;
import java.util.Objects;

public class CountingWell<E> implements Well<E> {

    private Well<E> innerWell;
    private int counter;

    public CountingWell(Well<E> innerWell) {
        this.innerWell = Objects.requireNonNull(innerWell, "innerWell must not be null!");
    }

    public int getCounter() {
        return counter;
    }

    @Override
    public boolean isConsumed() {
        if (innerWell == null) {
            return true;
        }
        return innerWell.isConsumed();
    }

    @Override
    public void close() {
        if (innerWell == null) {
            return;
        }
        Throwable throwable = null;
        try {
            innerWell.close();
        } catch (Throwable t) {
            throwable = t;
        }
        innerWell = null;
        if (throwable != null) {
            throw new WellException("Exception while closing innerWell!", throwable);
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new WellIterator();
    }

    private class WellIterator implements Iterator<E> {
        private final Iterator<E> iterator;

        WellIterator() {
            if (innerWell == null) {
                throw new WellException("Well was already closed!");
            }
            this.iterator = innerWell.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public E next() {
            if (innerWell == null) {
                throw new WellException("Well was already closed!");
            }
            E result = iterator.next();
            counter++;
            return result;
        }
    }
}
