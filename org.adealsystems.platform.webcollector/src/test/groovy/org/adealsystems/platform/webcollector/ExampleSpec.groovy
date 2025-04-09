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

package org.adealsystems.platform.webcollector

import org.adealsystems.platform.io.ListDrain
import org.adealsystems.platform.webcollector.example.CryptoId
import org.adealsystems.platform.webcollector.example.CryptoPriceQuery
import org.adealsystems.platform.webcollector.example.CryptoPrices
import spock.lang.Ignore
import spock.lang.Specification

@Ignore("This checks a real service")
class ExampleSpec extends Specification {
    def "collect some prices"() {
        given:
        HttpClientFactory clientFactory = new DefaultHttpClientFactory()
        HttpQuery<CryptoId, CryptoPrices> query = new CryptoPriceQuery()
        WebCollector<CryptoId, CryptoPrices> collector = new WebCollector<>(clientFactory, query, 100)
        List<CryptoId> queries = new ArrayList()
        queries.add(new CryptoId("bitcoin,ethereum,zcash"))
        queries.add(new CryptoId("bitcoin,ethereum,zcash"))
        queries.add(new CryptoId("bitcoin,ethereum,zcash"))
        queries.add(new CryptoId("bitcoin"))
        ListDrain<CryptoPrices> resultDrain = new ListDrain<>()

        when:
        collector.execute(queries, resultDrain)

        then:
        resultDrain.getContent().size() == 10

    }

    def "collect some prices with metrics"() {
        given:
        HttpClientFactory clientFactory = new DefaultHttpClientFactory()
        HttpQuery<CryptoId, CryptoPrices> query = new CryptoPriceQuery()
        WebCollector<CryptoId, CryptoPrices> collector = new WebCollector<>(clientFactory, query, 100)
        List<CryptoId> queries = new ArrayList()
        queries.add(new CryptoId("bitcoin,ethereum,zcash"))
        queries.add(new CryptoId("bitcoin,ethereum,zcash"))
        queries.add(new CryptoId("bitcoin,ethereum,zcash"))
        queries.add(new CryptoId("bitcoin"))
        ListDrain<CryptoPrices> resultDrain = new ListDrain<>()
        ListDrain<WebCollector.Metrics<CryptoId>> metricsDrain = new ListDrain<>()

        when:
        collector.execute(queries, resultDrain, metricsDrain)

        then:
        resultDrain.getContent().size() == 10
        metricsDrain.getContent().size() == 4
    }
}
