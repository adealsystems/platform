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

package org.adealsystems.platform.id.url;

import org.adealsystems.platform.id.DataInstance;
import org.adealsystems.platform.id.DataResolutionCapability;
import org.adealsystems.platform.id.DataResolutionStrategy;
import org.adealsystems.platform.id.NamingStrategy;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class UrlDataResolutionStrategy implements DataResolutionStrategy {
    private final String baseUrl;
    private final NamingStrategy namingStrategy;

    private static final Set<DataResolutionCapability> SUPPORTED = Collections.unmodifiableSet(new HashSet<>(Collections.singletonList(DataResolutionCapability.PATH)));

    public UrlDataResolutionStrategy(NamingStrategy namingStrategy, String baseUrl) {
        this.namingStrategy = Objects.requireNonNull(namingStrategy, "namingStrategy must not be null!");
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl must not be null!");
    }

    @Override
    public String getPath(DataInstance dataInstance) {
        return createUrlFor(dataInstance);
    }

    @Override
    public OutputStream getOutputStream(DataInstance dataInstance) {
        throw new UnsupportedOperationException("Streams are not supported by UrlDataResolutionStrategy!");
    }

    @Override
    public InputStream getInputStream(DataInstance dataInstance) {
        throw new UnsupportedOperationException("Streams are not supported by UrlDataResolutionStrategy!");
    }

    @Override
    public boolean delete(DataInstance dataInstance) {
        throw new UnsupportedOperationException("Delete is not supported by UrlDataResolutionStrategy!");
    }

    @Override
    public Set<DataResolutionCapability> getCapabilities() {
        return SUPPORTED;
    }

    private String createUrlFor(DataInstance dataInstance) {
        StringBuilder result = new StringBuilder();
        result.append(baseUrl);
        if (!baseUrl.endsWith("/")) {
            result.append('/');
        }
        result.append(namingStrategy.createLocalFilePart(dataInstance));
        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UrlDataResolutionStrategy that = (UrlDataResolutionStrategy) o;
        return baseUrl.equals(that.baseUrl) &&
            namingStrategy.equals(that.namingStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseUrl, namingStrategy);
    }

    @Override
    public String toString() {
        return "UrlDataResolutionStrategy{" +
            "baseUrl='" + baseUrl + '\'' +
            ", namingStrategy=" + namingStrategy +
            '}';
    }
}
