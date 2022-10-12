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
import org.adealsystems.platform.io.DrainFactory;
import org.adealsystems.platform.io.ListDrain;

/**
 * This class always returns the same ListDrain for testing purposes.
 */
public class DataObjectDrainFactory implements DrainFactory<String, DataObject> {
    private final ListDrain<DataObject> drainInstance = new ListDrain<>();
    private final BrokenDataObjectDrain brokenDrainInstance = new BrokenDataObjectDrain();

    public ListDrain<DataObject> getDrainInstance() {
        return drainInstance;
    }

    public BrokenDataObjectDrain getBrokenDrainInstance() {
        return brokenDrainInstance;
    }

    @Override
    public Drain<DataObject> createDrain(String input) {
        if ("fail".equals(input)) {
            throw new IllegalStateException("Failed to create Drain because this is a test!");
        }
        if ("null".equals(input)) {
            return null;
        }
        if ("broken".equals(input)) {
            return brokenDrainInstance;
        }

        return drainInstance;
    }
}
