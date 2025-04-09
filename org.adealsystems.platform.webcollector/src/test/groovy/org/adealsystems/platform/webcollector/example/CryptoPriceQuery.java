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

package org.adealsystems.platform.webcollector.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.adealsystems.platform.webcollector.HttpClientBundle;
import org.adealsystems.platform.webcollector.HttpQuery;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CryptoPriceQuery implements HttpQuery<CryptoId, CryptoPrices> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    @SuppressWarnings("PMD.CloseResource") // EntityUtils.consume(entity) does it.
    public List<CryptoPrices> perform(HttpClientBundle httpClientBundle, CryptoId query)
        throws IOException {
        List<CryptoPrices> result = new ArrayList<>();
        try {
            URI uri = new URIBuilder()
                .setScheme("https")
                .setHost("api.coingecko.com")
                .setPath("api/v3/simple/price")
                .addParameter("ids", query.getId())
                .addParameter("vs_currencies", "usd,eur,btc")
                .build();

            HttpGet httpGet = new HttpGet(uri.toString());
            try (CloseableHttpResponse response = httpClientBundle.getClient().execute(httpGet)) {
                int code = response.getCode();
                if (code != 200) {
                    throw new IOException("Expected status code 200 but got " + code + "!");
                }
                HttpEntity entity = response.getEntity();

                PriceResponse priceResponse = objectMapper.readValue(entity.getContent(), PriceResponse.class);
                for (Map.Entry<String, Map<String, Double>> current : priceResponse.entrySet()) {
                    String key = current.getKey();
                    Map<String, Double> value = current.getValue();
                    result.add(new CryptoPrices(key, value.get("usd"), value.get("eur"), value.get("btc")));
                }

                EntityUtils.consume(entity); // waving the dead chicken
            }
        } catch (URISyntaxException e) {
            throw new IOException("URI FAIL!", e);
        }
        return result;
    }

    @SuppressWarnings("checkstyle:IllegalType")
    private static class PriceResponse extends HashMap<String, Map<String, Double>> {
        private static final long serialVersionUID = -7242779284265985064L;
    }
}
