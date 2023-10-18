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

public enum Bucket {
    BUCKET_UNLOADING("ddl-unloading-point"),
    BUCKET_RUNTIME("ddl-runtime"),
    PROD_RAW("ddl-raw"),
    PROD_CURATED("ddl-curated"),
    PROD_REFINED("ddl-refined"),
    PROD_OUTGOING("ddl-outgoing"),
    UPCOMING_RAW("ddl-upcoming-raw"),
    UPCOMING_CURATED("ddl-upcoming-curated"),
    UPCOMING_REFINED("ddl-upcoming-refined"),
    UPCOMING_OUTGOING("ddl-upcoming-outgoing"),
    PREPROD_RAW("ddl-preprod-raw"),
    PREPROD_CURATED("ddl-preprod-curated"),
    PREPROD_REFINED("ddl-preprod-refined"),
    PREPROD_OUTGOING("ddl-preprod-outgoing"),
    SPL_RAW("ddl-spl-raw"),
    SPL_CURATED("ddl-spl-curated"),
    SPL_REFINED("ddl-spl-refined"),
    SPL_OUTGOING("ddl-spl-outgoing"),
    DEV_RAW("ddl-dev-raw"),
    DEV_CURATED("ddl-dev-curated"),
    DEV_REFINED("ddl-dev-refined"),
    DEV_OUTGOING("ddl-dev-outgoing");

    private final String realName;

    Bucket(String realName) {
        this.realName = realName;
    }

    public String getRealName() {
        return realName;
    }

    public static Optional<Bucket> resolveBucket(Environment environment, DataLakeZone zone) {
        Objects.requireNonNull(environment, "environment must not be null!");
        Objects.requireNonNull(zone, "bucket must not be null!");

        switch (environment) {
            case PROD:
                switch (zone) {
                    case RAW:
                        return Optional.of(Bucket.PROD_RAW);
                    case CURATED:
                        return Optional.of(Bucket.PROD_CURATED);
                    case REFINED:
                        return Optional.of(Bucket.PROD_REFINED);
                    case OUTGOING:
                        return Optional.of(Bucket.PROD_OUTGOING);
                    default:
                        return Optional.empty();
                }
            case UPCOMING:
                switch (zone) {
                    case RAW:
                        return Optional.of(Bucket.UPCOMING_RAW);
                    case CURATED:
                        return Optional.of(Bucket.UPCOMING_CURATED);
                    case REFINED:
                        return Optional.of(Bucket.UPCOMING_REFINED);
                    case OUTGOING:
                        return Optional.of(Bucket.UPCOMING_OUTGOING);
                    default:
                        return Optional.empty();
                }
            case PREPROD:
                switch (zone) {
                    case RAW:
                        return Optional.of(Bucket.PREPROD_RAW);
                    case CURATED:
                        return Optional.of(Bucket.PREPROD_CURATED);
                    case REFINED:
                        return Optional.of(Bucket.PREPROD_REFINED);
                    case OUTGOING:
                        return Optional.of(Bucket.PREPROD_OUTGOING);
                    default:
                        return Optional.empty();
                }
            case SPL:
                switch (zone) {
                    case RAW:
                        return Optional.of(Bucket.SPL_RAW);
                    case CURATED:
                        return Optional.of(Bucket.SPL_CURATED);
                    case REFINED:
                        return Optional.of(Bucket.SPL_REFINED);
                    case OUTGOING:
                        return Optional.of(Bucket.SPL_OUTGOING);
                    default:
                        return Optional.empty();
                }
            case DEV:
            case LOCAL:
                switch (zone) {
                    case RAW:
                        return Optional.of(Bucket.DEV_RAW);
                    case CURATED:
                        return Optional.of(Bucket.DEV_CURATED);
                    case REFINED:
                        return Optional.of(Bucket.DEV_REFINED);
                    case OUTGOING:
                        return Optional.of(Bucket.DEV_OUTGOING);
                    default:
                        return Optional.empty();
                }
            default:
                throw new IllegalStateException("Unknown/undefined environment '" + environment + "'!");
        }
    }

}
