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

package org.adealsystems.platform.orchestrator.status;


import org.adealsystems.platform.orchestrator.InternalEvent;

import java.util.Objects;

public class FileProcessingStep extends EventProcessingStep {
    private final String zone;
    private final String metaName;

    public static FileProcessingStep success(InternalEvent event, String zone, String metaName) {
        return new FileProcessingStep(true, event, buildDefaultMessage(metaName, zone), zone, metaName);
    }

    public static FileProcessingStep failed(InternalEvent event, String zone, String metaName) {
        return new FileProcessingStep(false, event, buildDefaultMessage(metaName, zone), zone, metaName);
    }

    public static FileProcessingStep failed(InternalEvent event, String zone, String metaName, String message) {
        return new FileProcessingStep(false, event, message, zone, metaName);
    }

    private static String buildDefaultMessage(String metaName, String zone) {
        if (metaName == null) {
            return "File arrived in zone " + zone;
        }
        return "File " + metaName + " arrived in zone " + zone;
    }

    public FileProcessingStep(boolean success, InternalEvent event, String message, String zone, String metaName) {
        super(success, event, message);
        this.zone = zone;
        this.metaName = metaName;
    }

    public String getZone() {
        return zone;
    }

    public String getMetaName() {
        return metaName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileProcessingStep)) return false;
        if (!super.equals(o)) return false;
        FileProcessingStep that = (FileProcessingStep) o;
        return Objects.equals(zone, that.zone) && Objects.equals(metaName, that.metaName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zone, metaName);
    }

    @Override
    public String toString() {
        return "FileProcessingStep{" +
            "zone=" + zone +
            ", metaName='" + metaName + '\'' +
            "}";
    }
}
