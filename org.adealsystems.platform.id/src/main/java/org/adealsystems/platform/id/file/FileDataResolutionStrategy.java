/*
 * Copyright 2020-2025 ADEAL Systems GmbH
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

package org.adealsystems.platform.id.file;

import org.adealsystems.platform.id.DataInstance;
import org.adealsystems.platform.id.DataResolutionCapability;
import org.adealsystems.platform.id.DataResolutionStrategy;
import org.adealsystems.platform.id.NamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Set;

public class FileDataResolutionStrategy implements DataResolutionStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileDataResolutionStrategy.class);

    private final File baseDirectory;
    private final NamingStrategy namingStrategy;

    public FileDataResolutionStrategy(NamingStrategy namingStrategy, File baseDirectory) {
        this.namingStrategy = Objects.requireNonNull(namingStrategy, "namingStrategy must not be null!");
        Objects.requireNonNull(baseDirectory, "baseDirectory must not be null!");
        if (!baseDirectory.exists()) {
            if (baseDirectory.mkdirs()) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Created base-directory '{}'.", baseDirectory.getAbsolutePath());
            } else {
                if (LOGGER.isWarnEnabled())
                    LOGGER.warn("Failed to create base-directory '{}'!", baseDirectory.getAbsolutePath());
            }
        }
        if (!baseDirectory.isDirectory()) {
            throw new IllegalArgumentException("baseDirectory '" + baseDirectory.getAbsolutePath() + "' is not a directory!");
        }
        this.baseDirectory = baseDirectory;
    }

    @Override
    public String getPath(DataInstance dataInstance) {
        return createFileFor(dataInstance).getAbsolutePath();
    }

    @Override
    public OutputStream getOutputStream(DataInstance dataInstance) throws IOException {
        File file = createFileFor(dataInstance);
        File parent = file.getParentFile();
        if (!parent.exists()) {
            if (parent.mkdirs()) {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("Created parent '{}'.", parent.getAbsolutePath());
            } else {
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Failed to create parent '{}'!", parent.getAbsolutePath());
            }
        }
        return Files.newOutputStream(Paths.get(file.getAbsolutePath()));
    }

    @Override
    public InputStream getInputStream(DataInstance dataInstance) throws IOException {
        return Files.newInputStream(Paths.get(createFileFor(dataInstance).getAbsolutePath()));
    }

    @Override
    public boolean delete(DataInstance dataInstance) {
        File f = createFileFor(dataInstance);
        return f.delete();
    }

    @Override
    public Set<DataResolutionCapability> getCapabilities() {
        return DataResolutionCapability.ALL;
    }

    private File createFileFor(DataInstance dataInstance) {
        return new File(baseDirectory, namingStrategy.createLocalFilePart(dataInstance));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileDataResolutionStrategy that = (FileDataResolutionStrategy) o;
        return baseDirectory.equals(that.baseDirectory) &&
            namingStrategy.equals(that.namingStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseDirectory, namingStrategy);
    }

    @Override
    public String toString() {
        return "FileDataResolutionStrategy{" +
            "baseDirectory=" + baseDirectory +
            ", namingStrategy=" + namingStrategy +
            '}';
    }
}
