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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;

public class BlockingQueueInternalEventQueue implements InternalEventSender, InternalEventReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingQueueInternalEventQueue.class);

    private final BlockingQueue<InternalEvent> queue;

    public BlockingQueueInternalEventQueue(BlockingQueue<InternalEvent> queue) {
        this.queue = Objects.requireNonNull(queue, "queue must not be null!");
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public boolean isBlocking() {
        return queue.remainingCapacity() == 0;
    }

    @Override
    public int getSize() {
        return queue.size();
    }

    @Override
    public void sendEvent(InternalEvent event) {
        Objects.requireNonNull(event, "event must not be null!");

        try {
            LOGGER.debug("Put a new event {}", event);
            queue.put(event);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new SendEventException(event, ex);
        }
    }

    @Override
    public Optional<InternalEvent> receiveEvent() {
        try {
            LOGGER.debug("Take a first available event");
            return Optional.of(queue.take());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    public InternalEvent peekEvent() {
        LOGGER.debug("Peek a first available event");
        return queue.peek();
    }
}
