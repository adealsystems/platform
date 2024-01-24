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

package org.adealsystems.platform.state.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.adealsystems.platform.id.DataFormat;
import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.id.DataInstance;
import org.adealsystems.platform.id.DataResolutionStrategy;
import org.adealsystems.platform.state.ProcessingState;
import org.adealsystems.platform.state.ProcessingStateException;
import org.adealsystems.platform.state.ProcessingStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;

public class ProcessingStateRepositoryImpl implements ProcessingStateRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingStateRepositoryImpl.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void setProcessingState(DataInstance dataInstance, ProcessingState state) {
        Objects.requireNonNull(state, "state must not be null!");
        DataInstance derived = deriveStateInstance(dataInstance);
        try (OutputStream os = derived.getOutputStream()) {
            OBJECT_MAPPER.writeValue(os, state);
        } catch (IOException e) {
            throw new ProcessingStateException("Exception while writing processing state for " + derived + "!", e);
        }
    }

    @Override
    public Optional<ProcessingState> getProcessingState(DataInstance dataInstance) {
        DataInstance derived = deriveStateInstance(dataInstance);
        try (InputStream is = derived.getInputStream()) {
            return Optional.of(OBJECT_MAPPER.readValue(is, ProcessingState.class));
        } catch (IOException e) {
            LOGGER.warn("Exception while reading processing state for {}!", derived, e);
            return Optional.empty();
        }
    }

    @Override
    public boolean removeProcessingState(DataInstance dataInstance) {
        DataInstance derived = deriveStateInstance(dataInstance);
        try {
            return derived.delete();
        } catch (IOException e) {
            throw new ProcessingStateException("Exception while deleting processing state for " + derived + "!", e);
        }
    }

    private static DataInstance deriveStateInstance(DataInstance dataInstance) {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        DataIdentifier original = dataInstance.getDataIdentifier();
        DataIdentifier dataIdentifier = new DataIdentifier(original.getSource(), original.getUseCase(), original.getConfiguration().orElse(null), DataFormat.STATE);
        DataResolutionStrategy dataResolutionStrategy = dataInstance.getDataResolutionStrategy();
        Optional<LocalDate> optionalDate = dataInstance.getDate();
        return new DataInstance(dataResolutionStrategy, dataIdentifier, optionalDate.orElse(null));
    }
}
