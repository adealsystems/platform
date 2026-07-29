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

package org.adealsystems.platform.http;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;

import java.net.http.HttpClient;

public interface AuthenticationTokenResolver {
    String resolveToken(HttpClient client);
    String resolveToken(CloseableHttpClient client);
    String refreshToken(HttpClient client, String rejectedToken);
    String refreshToken(CloseableHttpClient client, String rejectedToken);
}
