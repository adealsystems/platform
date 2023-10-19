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

package org.adealsystems.platform.orchestrator.registry;

import org.adealsystems.platform.id.DataIdentifier;

import java.util.Objects;
import java.util.regex.Pattern;

public final class FileEvent implements EventDescriptor {
    private final String id;
    private final String zone;
    private boolean startEvent;
    private boolean stopEvent;
    private Pattern pattern;
    private String metaName;
    private DataIdentifier dataId;

    public static FileEvent forIdAndZone(String id, String zone) {
        return new FileEvent(id, zone);
    }

    private FileEvent(String id, String zone) {
        this.id = Objects.requireNonNull(id, "id must not be null!");
        this.zone = zone;
    }

    public FileEvent asStartEvent() {
        this.startEvent = true;
        return this;
    }

    public FileEvent asStopEvent() {
        this.stopEvent = true;
        return this;
    }

    @Override
    public boolean isValid() {
        return pattern != null;
    }

    public FileEvent withPattern(String pattern) {
        this.pattern = Pattern.compile(Objects.requireNonNull(pattern, "pattern must not be null!"));
        return this;
    }

    public FileEvent withPattern(Pattern pattern) {
        this.pattern = Objects.requireNonNull(pattern, "pattern must not be null!");
        return this;
    }

    public FileEvent withMetaName(String metaName) {
        this.metaName = Objects.requireNonNull(metaName, "metaName must not be null!");
        return this;
    }

    public FileEvent withTargetDataIdentifier(DataIdentifier dataId) {
        this.dataId = Objects.requireNonNull(dataId, "dataId must not be null!");
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isStartEvent() {
        return startEvent;
    }

    @Override
    public boolean isStopEvent() {
        return stopEvent;
    }

    public String getZone() {
        return zone;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public String getMetaName() {
        return metaName;
    }

    public DataIdentifier getDataId() {
        return dataId;
    }
}
