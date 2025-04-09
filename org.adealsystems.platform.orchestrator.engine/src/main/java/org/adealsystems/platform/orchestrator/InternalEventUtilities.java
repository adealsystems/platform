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


import java.time.LocalDate;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class InternalEventUtilities {
    public static final String CLEANSER_DATE_FORMAT = "yyyy.MM.dd";

    private InternalEventUtilities() {
    }

    public static boolean isFileSizeZero(InternalEvent event) {
        Integer size = getFileSize(event);
        return size != null && size == 0;
    }

    public static boolean hasStateFileContent(InternalEvent event) {
        Integer size = getFileSize(event);
        return size != null && size > 2;
    }

    public static boolean isStateFile(InternalEvent event) {
        Pattern pattern = Pattern.compile("(.*)\\.state");
        String id = event.getId();
        Matcher matcher = pattern.matcher(id);
        return matcher.matches();
    }

    private static Integer getFileSize(InternalEvent event) {
        Optional<String> oSize = event.getAttributeValue(S3Constants.FILE_SIZE);
        if (oSize.isPresent()) {
            return Integer.parseInt(oSize.get());
        }

        return null;
    }

    public static LocalDate getEventDate(InternalEvent event) {
        String stringDate = event.getId().split("/")[0];
        return DateTimeUtility.parseDate(stringDate, CLEANSER_DATE_FORMAT);
    }

    public static Object getStateFileContent(InternalEvent event) {
        // TODO: implement me!
        return null;
    }
}
