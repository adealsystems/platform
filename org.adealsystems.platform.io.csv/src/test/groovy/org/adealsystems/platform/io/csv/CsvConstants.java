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

package org.adealsystems.platform.io.csv;

public interface CsvConstants {

    /**
     * This should be set to '&quot;' but this isn't working with commons-csv 1.9.0.
     *
     * @see <a href="https://issues.apache.org/jira/projects/CSV/issues/CSV-294">CSV-294</a>
     */
    Character CSV_ESCAPE_CHARACTER = null;

    char CSV_DELIMITER_SEMICOLON = ';';

}
