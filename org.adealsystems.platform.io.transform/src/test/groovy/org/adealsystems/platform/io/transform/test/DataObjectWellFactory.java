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

package org.adealsystems.platform.io.transform.test;

import org.adealsystems.platform.io.ListWell;
import org.adealsystems.platform.io.Well;
import org.adealsystems.platform.io.WellFactory;

import java.util.ArrayList;
import java.util.List;

public class DataObjectWellFactory implements WellFactory<String, DataObject> {

    @Override
    public Well<DataObject> createWell(String input) {
        if ("fail".equals(input)) {
            throw new IllegalStateException("Failed to create Well because this is a test!");
        }
        if ("null".equals(input)) {
            return null;
        }

        boolean nullEntries = "nullEntries".equals(input);

        List<DataObject> content = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            if (nullEntries && i % 2 == 0) {
                content.add(null);
            } else {
                content.add(new DataObject(i, input + " " + i));
            }
        }

        ListWell<DataObject> result = new ListWell<>(content);
        if ("broken".equals(input)) {
            return new BrokenDataObjectWell(result);
        }
        return result;
    }
}
