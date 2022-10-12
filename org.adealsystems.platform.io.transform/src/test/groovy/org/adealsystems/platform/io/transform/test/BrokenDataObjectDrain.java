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

import org.adealsystems.platform.io.Drain;
import org.adealsystems.platform.io.ListDrain;

import java.util.List;

public class BrokenDataObjectDrain implements Drain<DataObject> {

    private final ListDrain<DataObject> inner = new ListDrain<>();
    private int counter;

    public List<DataObject> getContent() {
        return inner.getContent();
    }

    @Override
    public void add(DataObject entry) {
        counter++;
        if (counter > 5) {
            throw new IllegalStateException("Failure because this is a test!");
        }
        inner.add(entry);
    }

    @Override
    public void addAll(Iterable<DataObject> entries) {
        for (DataObject entry : entries) {
            add(entry);
        }
    }

    @Override
    public void close() {
        inner.close();
    }
}
