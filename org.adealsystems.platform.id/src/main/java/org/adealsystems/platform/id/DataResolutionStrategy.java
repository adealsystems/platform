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

package org.adealsystems.platform.id;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Set;

public interface DataResolutionStrategy {
    String getPath(DataInstance dataInstance);

    OutputStream getOutputStream(DataInstance dataInstance) throws IOException;

    InputStream getInputStream(DataInstance dataInstance) throws IOException;

    boolean delete(DataInstance dataInstance) throws IOException;

    Set<DataResolutionCapability> getCapabilities();

    default boolean supports(DataResolutionCapability capability) {
        Objects.requireNonNull(capability, "capability must not be null!");
        return getCapabilities().contains(capability);
    }
}
