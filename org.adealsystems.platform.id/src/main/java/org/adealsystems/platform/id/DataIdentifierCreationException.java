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

package org.adealsystems.platform.id;

public class DataIdentifierCreationException extends IllegalArgumentException {
    private static final long serialVersionUID = -6400680999226215701L;

    private final String value;

    public DataIdentifierCreationException(String message) {
        super(message);
        this.value = null;
    }

    public DataIdentifierCreationException(String message, String value) {
        super(message);
        this.value = value;
    }

    public DataIdentifierCreationException(String message, String value, Throwable cause) {
        super(message, cause);
        this.value = value;
    }

    public String getValue() {
        return value;
    }


    @Override
    public String toString() {
        return "DataIdentifierCreationException{" +
            "value='" + value + '\'' +
            "} " + super.toString();
    }
}
