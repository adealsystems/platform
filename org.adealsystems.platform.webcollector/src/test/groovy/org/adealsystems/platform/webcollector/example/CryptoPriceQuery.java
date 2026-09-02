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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.adealsystems.platform.webcollector.HttpClientBundle;
import org.adealsystems.platform.webcollector.HttpQuery;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.io.Serial;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CryptoPriceQuery implements HttpQuery<CryptoId, CryptoPrices> {

    private final ObjectMapper objectMapper = new ObjectMapper();

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
                    objectMapper.readValue(entity.getContent(), PriceResponse.class);

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

    @SuppressWarnings("checkstyle:IllegalType")
    private static final class PriceResponse extends HashMap<String, Map<String, Double>> {
        @Serial
        private static final long serialVersionUID = -7242779284265985064L;
    }
}
