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

package org.adealsystems.platform.time;

import org.apache.commons.text.StringSubstitutor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DurationFormatter {
    private final int days;
    private final int hours;
    private final int minutes;
    private final int seconds;
    private final long millis;

    public static final char PARAM = '%';

    public static final char DAYS = 'd';
    public static final char HOURS = 'h';
    public static final char MINUTES = 'm';
    public static final char SECONDS = 's';
    public static final char MILLISECONDS = 'S';
    public static final List<Character> ORDERED_PARAMS = Arrays.asList(
        DAYS,
        HOURS,
        MINUTES,
        SECONDS,
        MILLISECONDS
    );

    public static final String PATTERN_VALUE = "([2-9][0-9]*)?" +
        '('
        + DAYS + '|'
        + HOURS + '|'
        + MINUTES + '|'
        + SECONDS + '|'
        + MILLISECONDS
        + ')'
        + "(.*)";
    public static final Pattern PATTERN = Pattern.compile(PATTERN_VALUE);

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

        Map<String, String> placeholders = new HashMap<>();
        int highestIndex = Integer.MAX_VALUE;

        StringBuilder result = new StringBuilder();
        int pos = value.indexOf(PARAM);
        while (pos >= 0) {
            if (pos == value.length() - 1) {
                // special handling: '%' is a last character
                break;
            }

            // add static part before placeholder
            String staticPart = value.substring(0, pos);
            result.append(staticPart);

            // remove leading '%'
            value = value.substring(pos + 1);

            char nextChar = value.charAt(0);
            if (nextChar == PARAM) {
                // '%%' - replace with a single '%'
                result.append(PARAM);
                value = value.substring(1);
                pos = value.indexOf(PARAM);
                continue;
            }

            Matcher matcher = PATTERN.matcher(value);
            if (!matcher.matches()) {
                result.append(PARAM);
                pos = value.indexOf(PARAM);
                continue;
            }

            if (matcher.groupCount() != 3) {
                throw new IllegalArgumentException("Expecting 2 or 3 regex groups, but found " + matcher.groupCount() + "!");
            }

            int length;
            String key;
            try {
                length = Integer.parseInt(matcher.group(1));
            }
            catch(NumberFormatException ex) {
                length = -1;
            }

            key = matcher.group(2);
            value = matcher.group(3);

            switch (key.charAt(0)) {
                case DAYS:
                    highestIndex = ORDERED_PARAMS.indexOf(DAYS);
                    placeholders.put("DAYS", formatValue(days, length));
                    result.append("${DAYS}");
                    break;
                case HOURS:
                    highestIndex = Math.min(highestIndex, ORDERED_PARAMS.indexOf(HOURS));
                    placeholders.put("HOURS", formatValue(hours, length));
                    result.append("${HOURS}");
                    break;
                case MINUTES:
                    highestIndex = Math.min(highestIndex, ORDERED_PARAMS.indexOf(MINUTES));
                    placeholders.put("MINUTES", formatValue(minutes, length));
                    result.append("${MINUTES}");
                    break;
                case SECONDS:
                    highestIndex = Math.min(highestIndex, ORDERED_PARAMS.indexOf(SECONDS));
                    placeholders.put("SECONDS", formatValue(seconds, length));
                    result.append("${SECONDS}");
                    break;
                case MILLISECONDS:
                    highestIndex = Math.min(highestIndex, ORDERED_PARAMS.indexOf(MILLISECONDS));
                    placeholders.put("MILLIS", formatValue(millis, length));
                    result.append("${MILLIS}");
                    break;
                default:
                    throw new IllegalStateException("Unexpected key '" + key + "'!");
            }

            pos = value.indexOf(PARAM);
        }

        result.append(value);

        switch (highestIndex) {
            case 0:
                // DAYS
                break;
            case 1:
                // HOURS
                if (days > 0) {
                    placeholders.put("HOURS", formatValue(
                        (long) days * 24
                            + hours, 2));
                }
                break;
            case 2:
                // MINUTES
                if (days > 0 || hours > 0) {
                    placeholders.put("MINUTES", formatValue(
                        (long) days * 24
                            + (long) hours * 60
                            + minutes, 2));
                }
                break;
            case 3:
                // SECONDS
                if (days > 0 || hours > 0 || minutes > 0) {
                    placeholders.put("SECONDS", formatValue(
                        (long) days * 24
                            + (long) hours * 60
                            + (long) minutes * 60
                            + seconds, 2));
                }
                break;
            case 4:
                // MILLIS
                if (days > 0 || hours > 0 || minutes > 0 || seconds > 0) {
                    placeholders.put("MILLIS", formatValue(
                        (long) days * 24
                            + (long) hours * 60
                            + (long) minutes * 60
                            + (long) seconds * 1000
                            + millis, 2));
                }
                break;
            default:
                // nothing to do
                break;
        }

        return StringSubstitutor.replace(result.toString(), placeholders);
    }

    private String formatValue(int value, int length) {
        if (length == -1) {
            return String.valueOf(value);
        }

        StringBuilder result = new StringBuilder();
        result.append(value);

        while (result.length() < length) {
            result.insert(0, "0");
        }

        return result.toString();
    }

    private String formatValue(long value, int length) {
        if (length == -1) {
            return String.valueOf(value);
        }

        StringBuilder result = new StringBuilder();
        result.append(value);

        while (result.length() < length) {
            result.insert(0, "0");
        }

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
