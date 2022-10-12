/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

package org.adealsystems.platform.sqlcollector;

import org.adealsystems.platform.io.csv.AbstractCsvDrain;
import org.adealsystems.platform.time.DurationFormatter;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.io.OutputStream;

public class SqlMetricsCsvDrain<Q> extends AbstractCsvDrain<SqlCollector.SqlMetrics<Q>> {

    public SqlMetricsCsvDrain(OutputStream outputStream, CSVFormat csvFormat) throws IOException {
        super(outputStream, csvFormat);
    }

    @Override
    public String getValue(SqlCollector.SqlMetrics<Q> metrics, String columnName) {
        switch (columnName) {
            case SqlCollector.SqlMetrics.COLUMN_ID:
                return metrics.getId();
            case SqlCollector.SqlMetrics.COLUMN_RESULT_COUNT:
                return String.valueOf(metrics.getResultCount());
            case SqlCollector.SqlMetrics.COLUMN_START_TIMESTAMP:
                return metrics.getStartTimestamp().toString();
            case SqlCollector.SqlMetrics.COLUMN_INIT_DURATION:
                DurationFormatter initDur = DurationFormatter.fromMillis(metrics.getInitDuration());
                return initDur.format("%m:%s");
            case SqlCollector.SqlMetrics.COLUMN_DELIVERY_DURATION:
                DurationFormatter deliveryDur = DurationFormatter.fromMillis(metrics.getDeliveryDuration());
                return deliveryDur.format("%m:%s");
            case SqlCollector.SqlMetrics.COLUMN_END_TIMESTAMP:
                return metrics.getEndTimestamp().toString();
            case SqlCollector.SqlMetrics.COLUMN_QUERY:
                return metrics.getQuery().toString();
            default:
                throw new IllegalArgumentException("Unknown/unsupported column name '" + columnName + "'!");
        }
    }
}
