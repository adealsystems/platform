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

package org.adealsystems.platform.id.hadoop;

import org.adealsystems.platform.id.DataInstance;
import org.adealsystems.platform.id.DataResolutionCapability;
import org.adealsystems.platform.id.DataResolutionStrategy;
import org.adealsystems.platform.id.NamingStrategy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Set;

public class HadoopDataResolutionStrategy implements DataResolutionStrategy {
    private final String baseUrl;
    private final NamingStrategy namingStrategy;
    private final FileSystem fileSystem;

    public HadoopDataResolutionStrategy(FileSystem fileSystem, NamingStrategy namingStrategy, String baseUrl) {
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem must not be null!");
        this.namingStrategy = Objects.requireNonNull(namingStrategy, "namingStrategy must not be null!");
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl must not be null!");
    }

    @Override
    public String getPath(DataInstance dataInstance) {
        return createUrlFor(dataInstance);
    }

    @Override
    public OutputStream getOutputStream(DataInstance dataInstance) throws IOException {
        return fileSystem.create(new Path(createUrlFor(dataInstance)));
    }

    @Override
    public InputStream getInputStream(DataInstance dataInstance) throws IOException {
        return fileSystem.open(new Path(createUrlFor(dataInstance)));
    }

    @Override
    public boolean delete(DataInstance dataInstance) throws IOException {
        return fileSystem.delete(new Path(createUrlFor(dataInstance)), true);
    }

    @Override
    public Set<DataResolutionCapability> getCapabilities() {
        return DataResolutionCapability.ALL;
    }

    private String createUrlFor(DataInstance dataInstance) {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        StringBuilder result = new StringBuilder();
        result.append(baseUrl);
        if (!baseUrl.endsWith("/")) {
            result.append('/');
        }
        result.append(namingStrategy.createLocalFilePart(dataInstance));
        return result.toString();
    }

    @Override
    public String toString() {
        return "HadoopDataResolutionStrategy{" +
            "baseUrl='" + baseUrl + '\'' +
            ", namingStrategy=" + namingStrategy +
            ", fileSystem=" + fileSystem +
            '}';
    }
}
