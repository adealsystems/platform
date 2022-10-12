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

package org.adealsystems.platform.io.transform.test;

import java.util.Iterator;

public class BrokenStringIterable implements Iterable<String> {
    @Override
    public Iterator<String> iterator() {
        return new BrokenIterator();
    }

    private static class BrokenIterator implements Iterator<String> {

        private int counter;

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            counter++;
            if (counter % 2 == 0) {
                throw new IllegalStateException("Exception while iterating inputs for test!");
            }
            return "input";
        }
    }
}
