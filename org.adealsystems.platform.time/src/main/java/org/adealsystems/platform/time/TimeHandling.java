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

package org.adealsystems.platform.time;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TimeHandling {
    public static final String YYYYMMDD_DATE_FORMAT = "yyyyMMdd";
    public static final DateTimeFormatter YYYYMMDD_DATE_FORMATTER = DateTimeFormatter.ofPattern(YYYYMMDD_DATE_FORMAT, Locale.US);

    public static final String YYYY_DOT_MM_DOT_DD_DATE_FORMAT = "yyyy.MM.dd";
    public static final DateTimeFormatter YYYY_DOT_MM_DOT_DD_DATE_FORMATTER = DateTimeFormatter.ofPattern(YYYY_DOT_MM_DOT_DD_DATE_FORMAT, Locale.US);

    public static final String YYYY_DASH_MM_DASH_DD_DATE_FORMAT = "yyyy-MM-dd";
    public static final DateTimeFormatter YYYY_DASH_MM_DASH_DD_DATE_FORMATTER = DateTimeFormatter.ofPattern(YYYY_DASH_MM_DASH_DD_DATE_FORMAT, Locale.US);

    public static final String DD_DOT_MM_DOT_YYYY_DATE_FORMAT = "dd.MM.yyyy";
    public static final DateTimeFormatter DD_DOT_MM_DOT_YYYY_DATE_FORMATTER = DateTimeFormatter.ofPattern(DD_DOT_MM_DOT_YYYY_DATE_FORMAT, Locale.US);

    public static final String WEEK_OF_YEAR_LONG_FORMAT = "yyyy'W'ww";
    public static final DateTimeFormatter WEEK_OF_YEAR_LONG_FORMATTER = DateTimeFormatter.ofPattern(WEEK_OF_YEAR_LONG_FORMAT, Locale.US);

    public static final String WEEK_OF_YEAR_SHORT_FORMAT = "yy'W'ww";
    public static final DateTimeFormatter WEEK_OF_YEAR_SHORT_FORMATTER = DateTimeFormatter.ofPattern(WEEK_OF_YEAR_SHORT_FORMAT, Locale.US);

    private static final Pattern TWO_DIGIT_WEEK_OF_YEAR_PATTERN = Pattern.compile("(\\d{2})W(\\d{2})");
    private static final Pattern FOUR_DIGIT_WEEK_OF_YEAR_PATTERN = Pattern.compile("(\\d{4})W(\\d{2})");

    private TimeHandling() {
    }

    /**
     * Returns a LocalDate (i.e. just year-month-day) for the given Date interpreted
     * in the given timezone.
     *
     * @param date   a Date
     * @param zoneId a ZoneId
     * @return a LocalDate
     */
    public static LocalDate toLocalDate(Date date, ZoneId zoneId) {
        Objects.requireNonNull(date, "date must not be null!");
        Objects.requireNonNull(zoneId, "zoneId must not be null!");
        // see https://stackoverflow.com/a/40143687/115167
        return date.toInstant().atZone(zoneId).toLocalDate();
    }

    /**
     * Returns a LocalDate (i.e. just year-month-day) for the given Date interpreted
     * in the system timezone.
     *
     * @param date a Date
     * @return a LocalDate
     */
    public static LocalDate toLocalDate(Date date) {
        return toLocalDate(date, ZoneId.systemDefault());
    }

    /**
     * Converts the given LocalDate to a Date at the start of the day
     * in the system timezone.
     *
     * @param localDate a LocalDate
     * @return a Date at the start of the day for the given ZoneId.
     */
    public static Date toDate(LocalDate localDate) {
        return toDate(localDate, ZoneId.systemDefault());
    }

    /**
     * Converts the given LocalDate to a Date at the start of the day
     * in the given timezone.
     *
     * @param localDate a LocalDate
     * @param zoneId    a ZoneId
     * @return a Date at the start of the day for the given ZoneId.
     */
    public static Date toDate(LocalDate localDate, ZoneId zoneId) {
        Objects.requireNonNull(localDate, "localDate must not be null!");
        Objects.requireNonNull(zoneId, "zoneId must not be null!");

        return Date.from(localDate.atStartOfDay(zoneId).toInstant());
    }


    // TODO: docs
    public static LocalDate convertToLocalDate(Object input) {
        if (input == null) {
            return null;
        }

        if (input instanceof LocalDate) {
            return (LocalDate) input;
        }

        if (input instanceof Number) {
            // Spark interprets columns with yyyyMMdd dates as Integer
            // So don't use dates like this, maybe
            String dateString = Long.toString(((Number) input).longValue());
            return LocalDate.parse(dateString, YYYYMMDD_DATE_FORMATTER);
        }

        if (input instanceof String) {
            String dateString = (String) input;
            if ("".equals(dateString)) {
                return null;
            }
            try {
                return LocalDate.parse(dateString, YYYY_DASH_MM_DASH_DD_DATE_FORMATTER);
            } catch (DateTimeParseException ex) {
                // ignore
            }
            try {
                return LocalDate.parse(dateString, YYYY_DOT_MM_DOT_DD_DATE_FORMATTER);
            } catch (DateTimeParseException ex) {
                // ignore
            }
            try {
                return LocalDate.parse(dateString, YYYYMMDD_DATE_FORMATTER);
            } catch (DateTimeParseException ex) {
                // ignore
            }
            try {
                return LocalDate.parse(dateString, DD_DOT_MM_DOT_YYYY_DATE_FORMATTER);
            } catch (DateTimeParseException ex) {
                // ignore
            }

            // invalid/unsupported string pattern
            return null;
        }

        if (input instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) input;
            return timestamp.toLocalDateTime()
                .toLocalDate();
        }

        if (input instanceof java.sql.Date) {
            // the rationale behind this very questionable piece of code is the
            // fact that java.sql.Date.toInstant() throws an UnsupportedOperationException
            // for extremely bogus reasons.
            //
            // this is fine. m(
            //
            // Liskov's notion of a behavioural subtype defines a notion of substitutability
            // for objects; that is, if S is a subtype of T, then objects of type T in a
            // program may be replaced with objects of type S without altering any of the
            // desirable properties of that program (e.g. correctness).
            //
            // - No new exceptions should be thrown by methods of the subtype, except where
            //   those exceptions are themselves subtypes of exceptions thrown by the methods
            //   of the supertype.
            //
            // ( https://en.wikipedia.org/wiki/Liskov_substitution_principle )
            input = new Date(((java.sql.Date) input).getTime());
        }

        if (input instanceof Date) {
            return toLocalDate((Date) input);
        }

        return null;
    }

    // TODO: docs
    public static Integer parseWeekFromWeekOfYear(String weekOfYear) {
        if (weekOfYear == null) {
            return null;
        }
        Matcher matcher = TWO_DIGIT_WEEK_OF_YEAR_PATTERN.matcher(weekOfYear);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(2));
        }

        matcher = FOUR_DIGIT_WEEK_OF_YEAR_PATTERN.matcher(weekOfYear);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(2));
        }
        throw new IllegalArgumentException(String.format(
            Locale.ROOT,
            "weekOfYear '%s' does not match either '%s' or '%s'!",
            weekOfYear, TWO_DIGIT_WEEK_OF_YEAR_PATTERN, FOUR_DIGIT_WEEK_OF_YEAR_PATTERN
        ));
    }

    // TODO: docs
    public static Integer parseYearFromWeekOfYear(String weekOfYear) {
        if (weekOfYear == null) {
            return null;
        }
        Matcher matcher = TWO_DIGIT_WEEK_OF_YEAR_PATTERN.matcher(weekOfYear);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(1)) + 2000; // TODO: Y2.1K bug ^^
        }

        matcher = FOUR_DIGIT_WEEK_OF_YEAR_PATTERN.matcher(weekOfYear);
        if (matcher.matches()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new IllegalArgumentException(String.format(
            Locale.ROOT,
            "weekOfYear '%s' does not match either '%s' or '%s'!",
            weekOfYear, TWO_DIGIT_WEEK_OF_YEAR_PATTERN, FOUR_DIGIT_WEEK_OF_YEAR_PATTERN
        ));
    }
}
