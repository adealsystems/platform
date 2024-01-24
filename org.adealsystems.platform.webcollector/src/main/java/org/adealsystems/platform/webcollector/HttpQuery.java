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

package org.adealsystems.platform.webcollector;

import java.io.IOException;
import java.util.List;

/**
 * Implementing classes must be thread-safe regarding their internal state.
 *
 * @param <Q> the Query type
 * @param <R> the Result type
 */
public interface HttpQuery<Q, R> {
    /**
     * Performs the given query using the given httpClient.
     * <p>
     * Implementations should not try to close the client as this is assumed
     * to be handled by the code calling this method.
     *
     * @param httpClientBundle contains the client used to perform the query
     * @param query            the Query instance
     * @return a List of Result instances
     * @throws IOException because HttpClient does so, too.
     */
    List<R> perform(HttpClientBundle httpClientBundle, Q query) throws IOException;
}
