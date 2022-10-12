/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

package org.adealsystems.platform.time;

import java.util.Objects;

public final class DurationFormatter {
    private final int days;
    private final int hours;
    private final int minutes;
    private final int seconds;
    private final long millis;

    public static final char PARAM = '%';

    public static final char DAYS = 'd';
    public static final char HOURS_12 = 'h';
    public static final char HOURS_24 = 'H';
    public static final char HOUR_ZONE = 't';
    public static final char MINUTES = 'm';
    public static final char SECONDS = 's';
    public static final char MILLISECONDS = 'S';

    private DurationFormatter(int days, int hours, int minutes, int seconds, long millis) {
        this.days = days;
        this.hours = hours;
        this.minutes = minutes;
        this.seconds = seconds;
        this.millis = millis;
    }

    public static DurationFormatter fromMillis(long millis) {
        if (millis < 1_000) {
            return new DurationFormatter(0, 0, 0, 0, millis);
        }

        long ms = millis % 1_000;
        long duration = millis / 1_000;
        if (duration < 60) {
            return new DurationFormatter(0, 0, 0, (int) duration, ms);
        }

        int seconds = (int) (duration % 60);
        duration /= 60;
        if (duration < 60) {
            return new DurationFormatter(0, 0, (int) duration, seconds, ms);
        }

        int minutes = (int) (duration % 60);
        duration /= 60;
        if (duration < 24) {
            return new DurationFormatter(0, (int) duration, minutes, seconds, ms);
        }

        int hours = (int) (duration % 24);
        duration /= 24;
        return new DurationFormatter((int) duration, hours, minutes, seconds, ms);
    }

    public String format(String pattern) {
        String value = pattern;

        StringBuilder result = new StringBuilder();
        int pos = value.indexOf(PARAM);
        while (pos >= 0) {
            String staticPart = value.substring(0, pos);
            result.append(staticPart);

            if (pos == value.length()) {
                result.append(PARAM);
                break;
            }

            char key = value.charAt(pos + 1);
            value = value.substring(pos + 2);

            switch (key) {
                case PARAM:
                    // '%%' - replace with a single '%'
                    result.append(PARAM);
                    break;
                case DAYS:
                    result.append(days);
                    break;
                case HOURS_12:
                    result.append((hours > 12) ? hours - 12 : hours);
                    break;
                case HOUR_ZONE:
                    result.append((hours > 12) ? "pm" : "am");
                    break;
                case HOURS_24:
                    result.append(hours);
                    break;
                case MINUTES:
                    result.append(minutes);
                    break;
                case SECONDS:
                    result.append(seconds);
                    break;
                case MILLISECONDS:
                    result.append(millis);
                    break;
                default:
                    result.append(PARAM).append(key);
                    break;
            }

            pos = value.indexOf(PARAM);
        }

        result.append(value);

        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DurationFormatter)) return false;
        DurationFormatter duration = (DurationFormatter) o;
        return days == duration.days
            && hours == duration.hours
            && minutes == duration.minutes
            && seconds == duration.seconds
            && millis == duration.millis;
    }

    @Override
    public int hashCode() {
        return Objects.hash(days, hours, minutes, seconds, millis);
    }

    @Override
    public String toString() {
        return "DurationFormatter{" +
            "days=" + days +
            ", hours=" + hours +
            ", minutes=" + minutes +
            ", seconds=" + seconds +
            ", millis=" + millis +
            '}';
    }
}
