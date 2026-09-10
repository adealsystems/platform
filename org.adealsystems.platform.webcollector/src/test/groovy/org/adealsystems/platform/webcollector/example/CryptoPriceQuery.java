/*
 * Copyright 2020-2026 ADEAL Systems GmbH
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

package org.adealsystems.platform.webcollector.example;

import org.adealsystems.platform.webcollector.HttpClientBundle;
import org.adealsystems.platform.webcollector.HttpQuery;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.net.URIBuilder;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

public class CryptoPriceQuery implements HttpQuery<CryptoId, CryptoPrices> {

    private static final JsonMapper JSON_MAPPER =
        JsonMapper.builder()
            .build();

    private static final TypeReference<Map<String, Map<String, Double>>> MAP_STRING_MAP_STRING_DOUBLE_TYPE_REFERENCE =
        new TypeReference<>() {
        };

    @Override
    public List<CryptoPrices> perform(HttpClientBundle httpClientBundle, CryptoId query)
        throws IOException {
        try {
            URI uri = new URIBuilder()
                .setScheme("https")
                .setHost("api.coingecko.com")
                .setPath("api/v3/simple/price")
                .addParameter("ids", query.getId())
                .addParameter("vs_currencies", "usd,eur,btc")
                .build();

            HttpGet httpGet = new HttpGet(uri.toString());

            return httpClientBundle.getClient().execute(httpGet, response -> {
                if (response.getCode() != 200) {
                    throw new IOException(
                        "Expected status code 200 but got " + response.getCode() + "!"
                    );
                }

                HttpEntity entity = response.getEntity();

                if (entity == null) {
                    throw new IOException("CoinGecko returned an empty response body!");
                }

                Map<String, Map<String, Double>> priceResponse =
                    JSON_MAPPER.readValue(entity.getContent(), MAP_STRING_MAP_STRING_DOUBLE_TYPE_REFERENCE);

                return priceResponse.entrySet()
                    .stream()
                    .map(entry -> {
                        Map<String, Double> value = entry.getValue();

                        return new CryptoPrices(
                            entry.getKey(),
                            value.get("usd"),
                            value.get("eur"),
                            value.get("btc")
                        );
                    })
                    .toList();
            });
        } catch (URISyntaxException e) {
            throw new IOException("URI FAIL!", e);
        }
    }
}
