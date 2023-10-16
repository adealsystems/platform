/*
 * Copyright 2020-2023 ADEAL Systems GmbH
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

package org.adealsystems.platform.spark.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

public final class SystemProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemProperties.class);

    private SystemProperties() {
    }

    public static void replaceProperty(String propertyName, String propertiesFileName) {
        Objects.requireNonNull(propertyName, "propertyName must not be null!");
        Objects.requireNonNull(propertiesFileName, "propertiesFileName must not be null!");

        String original = System.getProperty(propertyName);
        if (original == null) {
            return;
        }

        Properties replacementProperties = new Properties();
        try {
            loadPropertiesFrom(replacementProperties, propertiesFileName);
        } catch (IOException e) {
            LOGGER.warn("Failed to load properties from '{}'!", propertiesFileName, e);
        }
        String replacement = replacementProperties.getProperty(original);
        if (replacement == null) {
            return;
        }
        System.setProperty(propertyName, replacement);
    }

    public static void loadPropertiesFrom(Properties properties, String propertiesPath) throws IOException {
        Objects.requireNonNull(properties, "properties must not be null!");
        Objects.requireNonNull(propertiesPath, "propertiesPath must not be null!");
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (InputStream is = cl.getResourceAsStream(propertiesPath)) {
            if (is == null) {
                LOGGER.debug("Failed to obtain properties stream for '{}'!", propertiesPath);
                return;
            }
            properties.load(new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)));
        }
    }

}
