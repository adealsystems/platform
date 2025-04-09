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

package org.adealsystems.platform.webcollector;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Holds a CloseableHttpClient as well as a map for additional things
 * required by HttpQuery implementations.
 */
public final class HttpClientBundle {
    private final Map<String, Object> data;
    private final CloseableHttpClient client;

    public HttpClientBundle(CloseableHttpClient client) {
        this(client, null);
    }

    public HttpClientBundle(CloseableHttpClient client, Map<String, Object> data) {
        this.client = Objects.requireNonNull(client, "client must not be null!");
        this.data = data == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(data));
    }

    public Map<String, Object> getData() {
        return data;
    }

    public CloseableHttpClient getClient() {
        return client;
    }

    @Override
    public String toString() {
        return "HttpClientBundle{" +
            "data=" + data +
            ", client=" + client +
            '}';
    }
}
