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

package org.adealsystems.platform.memory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.adealsystems.platform.DataInstance;
import org.adealsystems.platform.DataResolutionStrategy;
import org.adealsystems.platform.NamingStrategy;

public class MemoryDataResolutionStrategy implements DataResolutionStrategy {
    private final NamingStrategy namingStrategy;
    private final Map<String, byte[]> data = new HashMap<>();

    public MemoryDataResolutionStrategy(NamingStrategy namingStrategy) {
        this.namingStrategy = Objects.requireNonNull(namingStrategy, "namingStrategy must not be null!");
    }

    @Override
    public Optional<String> getPath(DataInstance dataInstance) {
        return Optional.empty();
    }

    @Override
    public OutputStream getOutputStream(DataInstance dataInstance) {
        return new InnerOutputStream(namingStrategy.createLocalFilePart(dataInstance));
    }

    @Override
    public InputStream getInputStream(DataInstance dataInstance) throws IOException {
        String name = namingStrategy.createLocalFilePart(dataInstance);
        byte[] bytes = data.get(name);
        if (bytes == null) {
            throw new FileNotFoundException(name);
        }
        return new ByteArrayInputStream(bytes);
    }

    private class InnerOutputStream extends OutputStream {

        private final String name;
        private final ByteArrayOutputStream bos;

        InnerOutputStream(String name) {
            this.name = name;
            this.bos = new ByteArrayOutputStream();
        }

        @Override
        public void write(int b) {
            bos.write(b);
        }

        @Override
        public void close() throws IOException {
            super.close();
            data.put(name, bos.toByteArray());
        }
    }

    // I did not forget about equals()/hashCode()
    // There's no way to implement it efficiently so instances
    // are never equal to other instances.

    @Override
    public String toString() {
        return "MemoryDataResolutionStrategy{" +
                "namingStrategy=" + namingStrategy +
                '}';
    }
}
