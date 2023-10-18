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

import java.util.Collections;
import java.util.Map;

public class MetaName {
    private final String name;
    private final Map<String, String> attributes;
    public static final String RMS_GROUP_KEY = "rmsGroup";
    public static final String RUN_COUNT_KEY = "runCount";
    public static final String RMS_GROUP_SEASON_KEY = "rmsGroupSeason";
    public static final String IATA_CODE_SEASON_KEY = "iataCodeSeason";
    public static final String ACTIVE_RMS_GROUPS_KEY = "activeRmsGroups";
    public static final String ACTIVE_RMS_GROUP_SEASONS_KEY = "activeRmsGroupSeasons";

    public MetaName() {
        this.name = null;
        this.attributes = Collections.emptyMap();
    }

    public MetaName(String name, Map<String, String> attributes) {
        this.name = name;
        this.attributes = attributes;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
