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
import java.util.Spliterator;
import java.util.function.Consumer;

public final class ListWell<E> implements Well<E> {

    private List<E> content;
    private boolean consumed;

    public ListWell() {
        this(new ArrayList<>());
    }

    public ListWell(List<E> content) {
        setContent(content);
    }

    /**
     * Returns the content of this Well or null if Well was already closed.
     *
     * @return content of this Well or null if Well was already closed.
     */
    public List<E> getContent() {
        return content;
    }

    /**
     * Sets the content of this Well.
     *
     * @param content the content of this Well.
     * @throws NullPointerException if content is null
     */
    public void setContent(List<E> content) {
        this.content = Objects.requireNonNull(content, "content must not be null!");
    }

    @Override
    public boolean isConsumed() {
        return consumed;
    }

    @Override
    public void close() {
        content = null;
    }

    @Override
    public Iterator<E> iterator() {
        if (content == null) {
            throw new IllegalStateException("Well was already closed!");
        }

        if (consumed) {
            throw new WellException("A well can only be iterated once!");
        }

        consumed = true;
        return content.iterator();
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        if (content == null) {
            throw new IllegalStateException("Well was already closed!");
        }
        content.forEach(action);
    }

    @Override
    public Spliterator<E> spliterator() {
        if (content == null) {
            throw new IllegalStateException("Well was already closed!");
        }
        return content.spliterator();
    }
}
