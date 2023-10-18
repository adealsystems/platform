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

import java.util.Objects;
import java.util.Optional;

public enum DataLakeZone {
    UNLOADING,
    RAW,
    CURATED,
    REFINED,
    OUTGOING;

    public static Optional<DataLakeZone> resolveDataLakeZone(Environment environment, String bucketName) {
        Objects.requireNonNull(environment, "environment must not be null!");
        Objects.requireNonNull(bucketName, "bucketName must not be null!");

        if ("ddl-unloading-point".equals(bucketName)) {
            return Optional.of(DataLakeZone.UNLOADING);
        }

        switch (environment) {
            case PROD:
                switch (bucketName) {
                    case "ddl-raw":
                        return Optional.of(DataLakeZone.RAW);
                    case "ddl-curated":
                        return Optional.of(DataLakeZone.CURATED);
                    case "ddl-refined":
                    case "ddl-refined-replication":
                        return Optional.of(DataLakeZone.REFINED);
                    case "ddl-outgoing":
                        return Optional.of(DataLakeZone.OUTGOING);
                    default:
                        return Optional.empty();
                }
            case UPCOMING:
                switch (bucketName) {
                    case "ddl-upcoming-raw":
                        return Optional.of(DataLakeZone.RAW);
                    case "ddl-upcoming-curated":
                        return Optional.of(DataLakeZone.CURATED);
                    case "ddl-upcoming-refined":
                    case "ddl-upcoming-refined-replication":
                        return Optional.of(DataLakeZone.REFINED);
                    case "ddl-upcoming-outgoing":
                        return Optional.of(DataLakeZone.OUTGOING);
                    default:
                        return Optional.empty();
                }
            case PREPROD:
                switch (bucketName) {
                    case "ddl-preprod-raw":
                        return Optional.of(DataLakeZone.RAW);
                    case "ddl-preprod-curated":
                        return Optional.of(DataLakeZone.CURATED);
                    case "ddl-preprod-refined":
                    case "ddl-preprod-refined-replication":
                        return Optional.of(DataLakeZone.REFINED);
                    case "ddl-preprod-outgoing":
                        return Optional.of(DataLakeZone.OUTGOING);
                    default:
                        return Optional.empty();
                }
            case SPL:
                switch (bucketName) {
                    case "ddl-spl-raw":
                        return Optional.of(DataLakeZone.RAW);
                    case "ddl-spl-curated":
                        return Optional.of(DataLakeZone.CURATED);
                    case "ddl-spl-refined":
                    case "ddl-spl-refined-replication":
                        return Optional.of(DataLakeZone.REFINED);
                    case "ddl-spl-outgoing":
                        return Optional.of(DataLakeZone.OUTGOING);
                    default:
                        return Optional.empty();
                }
            case DEV:
            case LOCAL:
                switch (bucketName) {
                    case "ddl-dev-raw":
                        return Optional.of(DataLakeZone.RAW);
                    case "ddl-dev-curated":
                        return Optional.of(DataLakeZone.CURATED);
                    case "ddl-dev-refined":
                    case "ddl-dev-refined-replication":
                        return Optional.of(DataLakeZone.REFINED);
                    case "ddl-dev-outgoing":
                        return Optional.of(DataLakeZone.OUTGOING);
                    default:
                        return Optional.empty();
                }
            default:
                throw new IllegalStateException("Unknown/undefined environment '" + environment + "'!");
        }
    }

    }
