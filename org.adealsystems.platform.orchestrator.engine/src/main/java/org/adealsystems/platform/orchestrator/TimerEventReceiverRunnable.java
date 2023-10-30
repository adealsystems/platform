/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Objects;

public class TimerEventReceiverRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimerEventReceiverRunnable.class);

    private final InternalEventSender eventSender;

    private static final Duration TIMER_STEP = Duration.of(1, ChronoUnit.MINUTES);
    private static final DateTimeFormatter TIMER_FORMATTER = DateTimeFormatter.ofPattern("HH:mm", Locale.ROOT);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT);

    private LocalDateTime lastTimestamp = null;

    public TimerEventReceiverRunnable(InternalEventSender eventSender) {
        this.eventSender = Objects.requireNonNull(eventSender, "eventSender must not be null!");
    }

    @Override
    public void run() {
        LOGGER.info("Starting TIMER event receiver thread");

        // should be started as daemon thread
        while (true) {
            LocalDateTime timestamp = LocalDateTime.now(ZoneId.systemDefault());

            if (lastTimestamp == null) {
                LOGGER.debug("Initializing timer with {} ...", timestamp);
                lastTimestamp = timestamp;
            }
            else {
                Duration duration = Duration.between(lastTimestamp, timestamp);
                LOGGER.debug("Comparing timestamp {} with {} ...", lastTimestamp, timestamp);
                if (duration.compareTo(TIMER_STEP) >= 0) {
                    String eventId = TIMER_FORMATTER.format(timestamp);
                    String date = DATE_FORMATTER.format(timestamp);
                    String dayOfMonth = String.valueOf(timestamp.get(ChronoField.DAY_OF_MONTH));
                    String dayOfWeek = String.valueOf(timestamp.get(ChronoField.DAY_OF_WEEK));

                    // send the next event
                    InternalEvent timerEvent = new InternalEvent(); // NOPMD
                    timerEvent.setId(eventId);
                    timerEvent.setType(InternalEventType.TIMER);
                    timerEvent.setTimestamp(timestamp);
                    timerEvent.setAttributeValue("date", date);
                    timerEvent.setAttributeValue("day-of-week", dayOfWeek);
                    timerEvent.setAttributeValue("day-of-month", dayOfMonth);

                    InternalEvent cancelEvent = new InternalEvent(); // NOPMD
                    cancelEvent.setId(eventId);
                    cancelEvent.setType(InternalEventType.CANCEL);
                    cancelEvent.setTimestamp(timestamp);
                    cancelEvent.setAttributeValue("date", date);
                    cancelEvent.setAttributeValue("day-of-week", dayOfWeek);
                    cancelEvent.setAttributeValue("day-of-month", dayOfMonth);

                    LOGGER.debug("Sending a new timer/cancel events {}, {}", timerEvent, cancelEvent);
                    try {
                        eventSender.sendEvent(timerEvent);
                        eventSender.sendEvent(cancelEvent);
                    } catch (Exception ex) {
                        LOGGER.error("Error sending events {}/{}!", timerEvent, cancelEvent, ex);
                    }

                    // update last timestamp
                    lastTimestamp = timestamp;
                }
            }

            try {
                sleep(10_000);
            } catch (InterruptedException ex) {
                break;
            }
        }
    }

    private void sleep(long value) throws InterruptedException {
        Thread.sleep(value);
    }
}
