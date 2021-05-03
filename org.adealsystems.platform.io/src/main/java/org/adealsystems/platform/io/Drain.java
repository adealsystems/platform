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

/**
 *
 * @param <E> the type this Drain can handle.
 */
public interface Drain<E> extends AutoCloseable {
    /**
     * Drains the given entry.
     *
     * @param entry the entry that will be drained.
     * @throws NullPointerException  if the given entry is null.
     * @throws IllegalStateException if this method is called after the Drain was closed.
     */
    void add(E entry);

    /**
     * Drains the given entries.
     *
     * @param entries the entries that will be drained.
     * @throws NullPointerException  if the given iterable or a contained value is null.
     * @throws IllegalStateException if this method is called after the Drain was closed.
     */
    void addAll(Iterable<E> entries);
}