/*
 * Copyright 2020 ADEAL Systems GmbH
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

package org.adealsystems.platform.url;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Optional;

import org.adealsystems.platform.DataInstance;
import org.adealsystems.platform.DataResolutionStrategy;
import org.adealsystems.platform.NamingStrategy;

public class UrlDataResolutionStrategy implements DataResolutionStrategy {
    private final String baseUrl;
    private final NamingStrategy namingStrategy;

    public UrlDataResolutionStrategy(NamingStrategy namingStrategy, String baseUrl) {
        this.namingStrategy = Objects.requireNonNull(namingStrategy, "namingStrategy must not be null!");
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl must not be null!");
    }

    @Override
    public Optional<String> getPath(DataInstance dataInstance) {
        return Optional.of(createUrlFor(dataInstance));
    }

    @Override
    public OutputStream getOutputStream(DataInstance dataInstance) throws IOException {
        throw new IOException("Streams are not supported by UrlDataResolutionStrategy!");
    }

    @Override
    public InputStream getInputStream(DataInstance dataInstance) throws IOException {
        throw new IOException("Streams are not supported by UrlDataResolutionStrategy!");
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
