/*
 * Copyright 2020-2021 ADEAL Systems GmbH
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
import java.util.List;
import java.util.Objects;

/**
 * Drains to a contained List.
 *
 * @param <E> the type this Drain can handle.
 */
public class ListDrain<E> implements Drain<E> {

    private boolean closed;
    private final List<E> content = new ArrayList<>();

    @Override
    public void add(E entry) {
        if (closed) {
            throw new IllegalStateException("Drain was already closed!");
        }
        content.add(Objects.requireNonNull(entry, "entry must not be null!"));
    }

    @Override
    public void addAll(Iterable<E> entries) {
        Objects.requireNonNull(entries, "entries must not be null!");
        if (closed) {
            throw new IllegalStateException("Drain was already closed!");
        }
        for(E entry : entries) {
            content.add(Objects.requireNonNull(entry, "entries must not contain null!"));
        }
    }

    public List<E> getContent() {
        return content;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public String toString() {
        return "ListDrain{" +
                "content=" + content +
                '}';
    }
}
