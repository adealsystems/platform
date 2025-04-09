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

package org.adealsystems.platform.orchestrator.executor;

import de.huxhorn.sulky.ulid.ULID;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

public class UlidCommandIdGenerator implements CommandIdGenerator {

    private final ULID ulid = new ULID();

    private final ReentrantLock lock = new ReentrantLock();

    private ULID.Value previousUlid;

    @Override
    public String generate() {
        lock.lock();
        try {
            if (previousUlid == null) {
                previousUlid = ulid.nextValue();
                return previousUlid.toString();
            }

            for (; ; ) {
                Optional<ULID.Value> oNextUlid = ulid.nextStrictlyMonotonicValue(previousUlid);
                if (oNextUlid.isPresent()) {
                    previousUlid = oNextUlid.get();
                    return previousUlid.toString();
                }

                try {
                    //noinspection BusyWait
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    // ignore
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
