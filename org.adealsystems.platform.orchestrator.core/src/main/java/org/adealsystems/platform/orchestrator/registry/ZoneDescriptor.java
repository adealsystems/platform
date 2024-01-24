/*
 * Copyright 2020-2024 ADEAL Systems GmbH
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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class ZoneDescriptor {
    String zone;
    Set<FileDescriptor> files = new HashSet<>();

    private ZoneDescriptor(String zone) {
        this.zone = zone;
    }

    public static ZoneDescriptor forZone(String zone) {
        return new ZoneDescriptor(zone);
    }

    public ZoneDescriptor with(Pattern pattern) {
        this.files.add(new FileDescriptor(pattern, null, null));
        return this;
    }

    public ZoneDescriptor with(Pattern pattern, String metaName) {
        this.files.add(new FileDescriptor(pattern, metaName, null));
        return this;
    }

    public ZoneDescriptor with(String patternValue, String metaName) {
        this.files.add(new FileDescriptor(Pattern.compile(patternValue), metaName, null));
        return this;
    }

    public ZoneDescriptor with(Pattern pattern, String metaName, DataIdentifier dataId) {
        this.files.add(new FileDescriptor(pattern, metaName, dataId));
        return this;
    }

    public ZoneDescriptor with(String patternValue, String metaName, DataIdentifier dataId) {
        this.files.add(new FileDescriptor(Pattern.compile(patternValue), metaName, dataId));
        return this;
    }

    public String getZone() {
        return zone;
    }

    public Set<FileDescriptor> getFiles() {
        return files;
    }

    public static class FileDescriptor {
        Pattern pattern;
        String metaName;
        DataIdentifier dataId;

        public FileDescriptor(Pattern pattern, String metaName, DataIdentifier dataId) {
            this.pattern = pattern;
            this.metaName = metaName;
            this.dataId = dataId;
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
}
