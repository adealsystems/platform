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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class BrokenDataObjectList extends ArrayList<DataObject> {
    private static final long serialVersionUID = -5278998080841342241L;

    public BrokenDataObjectList(Collection<? extends DataObject> c) {
        super(c);
    }

    @Override
    public Iterator<DataObject> iterator() {
        return new BrokenIterator(super.iterator());
    }

    private static class BrokenIterator implements Iterator<DataObject> {

        private final Iterator<DataObject> iterator;

        BrokenIterator(Iterator<DataObject> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public DataObject next() {
            return iterator.next();
        }
    }
}
