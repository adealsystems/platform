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

package org.adealsystems.platform.orchestrator;

public class InstanceIdCreationException extends IllegalArgumentException {
    private static final long serialVersionUID = -8046035637706946926L;

    private final String value;

    public InstanceIdCreationException(String message) {
        super(message);
        this.value = null;
    }

    public InstanceIdCreationException(String message, String value) {
        super(message);
        this.value = value;
    }

    public InstanceIdCreationException(String message, String value, Throwable cause) {
        super(message, cause);
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return "InstanceIdCreationException{" +
            "value='" + value + '\'' +
            "} " + super.toString();
    }
}
