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

package org.adealsystems.platform.spark.main;

import org.adealsystems.platform.spark.DatasetLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
public class DatasetLoggerInit implements ApplicationListener<ContextRefreshedEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetLoggerInit.class);

    @Value("${dataset.logger.disabled}")
    boolean datasetLoggerDisabled;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent ignored) {
        DatasetLogger.setDisabledGlobally(datasetLoggerDisabled);
        if (LOGGER.isInfoEnabled())
            LOGGER.info("DatasetLogger.isDisabledGlobally(): {}", DatasetLogger.isDisabledGlobally());
    }
}
