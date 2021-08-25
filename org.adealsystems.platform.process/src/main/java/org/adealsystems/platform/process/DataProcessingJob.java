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

package org.adealsystems.platform.process;

import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.id.DataInstance;
import org.adealsystems.platform.id.DataResolver;

import java.util.Map;
import java.util.Set;

public interface DataProcessingJob {

    String STATE_PROCESSING_ERROR = "processing-error";

    long getStartTimestamp();

    long getDuration();

    Map<DataIdentifier, Set<DataInstance>> getInputInstances();

    Set<DataIdentifier> getOutputIdentifiers();

    WriteMode getWriteMode();

    DataResolver getOutputDataResolver();

    Map<DataIdentifier, String> getProcessingStatus();
    void registerProcessingStatus(DataIdentifier dataIdentifier, String status);

    void execute();
}
