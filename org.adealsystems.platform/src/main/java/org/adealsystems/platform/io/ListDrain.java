/*
 * Copyright 2020 ADEAL Systems GmbH
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

public class ListDrain<E> implements Drain<E> {

    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean closed;
    private final List<E> content = new ArrayList<>();

    @Override
    public void add(E entry) {
        Objects.requireNonNull(entry, "entry must not be null!");
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Drain was already closed!");
            }
            content.add(entry);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @SuppressWarnings("PMD.AvoidCatchingNPE")
    public void addAll(Collection<? extends E> collection) {
        Objects.requireNonNull(collection, "collection must not be null!");
        if (collection.isEmpty()) {
            return;
        }
        try {
            if (collection.contains(null)) {
                throw new IllegalArgumentException("collection must not contain null!");
            }
        } catch (NullPointerException ex) {
            // ignore
            //
            // this can happen if the collection does not support null values, but
            // usually doesn't.
            //
            // Someone considered this acceptable (optional) behaviour while defining
            // the Collection API... instead of just, you know, returning
            // false if the Collection does not support null values.
            // Which would be the obviously correct answer.
            //
            // But I digress...
        }
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Drain was already closed!");
            }
            content.addAll(collection);
        } finally {
            lock.unlock();
        }
    }

    public List<E> getContent() {
        return content;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            lock.unlock();
        }
    }
}
