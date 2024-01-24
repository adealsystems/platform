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
import org.adealsystems.platform.state.ProcessingState;
import org.adealsystems.platform.state.ProcessingStateException;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Objects;

public final class ProcessingStateFileWriter {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ProcessingStateFileWriter() {
    }

    public static void write(File file, ProcessingState state) {
        Objects.requireNonNull(state, "state must not be null!");
        Objects.requireNonNull(file, "file must not be null!");

        try (OutputStream os = Files.newOutputStream(file.toPath())) {
            OBJECT_MAPPER.writeValue(os, state);
        } catch (IOException e) {
            throw new ProcessingStateException("Exception while writing processing state " + state + "!", e);
        }
    }
}
