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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public final class ListWell<E> implements Well<E> {

    private final List<E> content;
    private boolean closed;
    private boolean consumed;

    public ListWell() {
        this(new ArrayList<>());
    }

    public ListWell(List<E> content) {
        this.content = Objects.requireNonNull(content, "content must not be null!");
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    @Override
    public void close() {
        closed = true;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<E> iterator() {
        if (closed) {
            throw new WellException("Well was already closed!");
        }

        if (consumed) {
            throw new WellException("A well can only be iterated once!");
        }

        consumed = true;
        return content.iterator();
    }

    /**
     * Returns the content of this Well.
     *
     * @return the content of this Well
     */
    public List<E> getContent() {
        return content;
    }

    /**
     * Resets closed and consumed state.
     * <p>
     * This does not clear the content! You can do so yourself by executing
     * <code>getContent().clear()</code>.
     */
    public void reset() {
        closed = false;
        consumed = false;
    }

    @Override
    public String toString() {
        return "ListWell{" +
            "content=" + content +
            '}';
    }
}
