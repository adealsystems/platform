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

package org.adealsystems.platform.webcollector.example;

import java.util.Objects;

public class CryptoPrices {
    private final String id;
    private final Double asUsd;
    private final Double asEur;
    private final Double asBtc;

    public CryptoPrices(String id, Double asUsd, Double asEur, Double asBtc) {
        this.id = Objects.requireNonNull(id);
        this.asUsd = asUsd;
        this.asEur = asEur;
        this.asBtc = asBtc;
    }

    public String getId() {
        return id;
    }

    public Double getAsUsd() {
        return asUsd;
    }

    public Double getAsEur() {
        return asEur;
    }

    public Double getAsBtc() {
        return asBtc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CryptoPrices that = (CryptoPrices) o;
        return Objects.equals(id, that.id) && Objects.equals(asUsd, that.asUsd) && Objects.equals(asEur, that.asEur) && Objects.equals(asBtc, that.asBtc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, asUsd, asEur, asBtc);
    }

    @Override
    public String toString() {
        return "CryptoPrices{" +
            "id='" + id + '\'' +
            ", asUsd=" + asUsd +
            ", asEur=" + asEur +
            ", asBtc=" + asBtc +
            '}';
    }
}
