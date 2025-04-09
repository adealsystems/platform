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

package org.adealsystems.platform.spark.udf;

import org.adealsystems.platform.time.TimeHandling;
import org.apache.spark.sql.api.java.UDF1;

import java.time.LocalDate;
import java.util.Locale;
import java.util.Objects;

public class WeekOfYearOfDateUDF implements UDF1<Object, String> {
    private static final long serialVersionUID = -8734875778324463949L;

    private final boolean longFormat;
    private final Locale locale;

    public WeekOfYearOfDateUDF(Locale locale) {
        this(true, locale);
    }

    public WeekOfYearOfDateUDF(boolean longFormat, Locale locale) {
        this.longFormat = longFormat;
        this.locale = Objects.requireNonNull(locale, "locale must not be null!");
    }

    @Override
    public String call(Object input) {
        LocalDate date = TimeHandling.convertToLocalDate(input);
        if (date == null) {
            return null;
        }
        if (longFormat) {
            return date.format(TimeHandling.WEEK_OF_YEAR_LONG_FORMATTER.withLocale(locale));
        }
        return date.format(TimeHandling.WEEK_OF_YEAR_SHORT_FORMATTER.withLocale(locale));
    }
}
