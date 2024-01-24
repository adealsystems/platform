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

package org.adealsystems.platform.io.transform.test;

import org.adealsystems.platform.io.Well;

import java.util.Iterator;
import java.util.Objects;

public class BrokenDataObjectWell implements Well<DataObject> {
    private final Well<DataObject> inner;

    public BrokenDataObjectWell(Well<DataObject> inner) {
        this.inner = Objects.requireNonNull(inner, "inner must not be null!");
    }

    @Override
    public boolean isConsumed() {
        return inner.isConsumed();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public Iterator<DataObject> iterator() {
        return new BrokenDataObjectWell.WellIterator(inner.iterator());
    }

    private static class WellIterator implements Iterator<DataObject> {

        private final Iterator<DataObject> inner;
        private int counter;

        WellIterator(Iterator<DataObject> inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public DataObject next() {
            counter++;
            if (counter > 5) {
                throw new IllegalStateException("Exception while reading from test Well.");
            }
            return inner.next();
        }
    }
}
