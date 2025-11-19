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

package org.adealsystems.platform.orchestrator;

import java.time.LocalDateTime;
import java.util.Comparator;

public class TimestampAwareComparator implements Comparator<TimestampAware> {
    @Override
    public int compare(TimestampAware o1, TimestampAware o2) {
        if (o1 == null && o2 == null) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
        LocalDateTime t1 = o1.getTimestamp();
        LocalDateTime t2 = o2.getTimestamp();
        if (t1 == t2) { // NOPMD CompareObjectsWithEquals
            return 0;
        }
        if (t1 == null) {
            return -1;
        }
        if (t2 == null) {
            return 1;
        }

        return t1.compareTo(t2);    }
}
