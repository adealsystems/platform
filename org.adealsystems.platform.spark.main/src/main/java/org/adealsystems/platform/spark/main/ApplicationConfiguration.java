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

package org.adealsystems.platform.spark.main;

import org.adealsystems.platform.DataLocation;
import org.adealsystems.platform.DataResolver;
import org.adealsystems.platform.DataResolverRegistry;
import org.adealsystems.platform.DefaultNamingStrategy;
import org.adealsystems.platform.MapDataResolverRegistry;
import org.adealsystems.platform.NamingStrategy;
import org.adealsystems.platform.TimeHandling;
import org.adealsystems.platform.file.FileDataResolutionStrategy;
import org.adealsystems.platform.url.UrlDataResolutionStrategy;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

import java.io.File;
import java.time.Clock;
import java.time.LocalDate;
import java.util.Objects;

@Configuration
public class ApplicationConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationConfiguration.class);

    @Value("${data.location.base.directory.input}")
    String inputBaseDirectory;

    @Value("${data.location.base.directory.output}")
    String outputBaseDirectory;

    @Bean
    LocalDate invocationDate(@Value("${invocation.date:#{null}}") String invocationDateString) {
        if (invocationDateString != null) {
            LocalDate invocationDate = LocalDate.parse(invocationDateString, TimeHandling.YYYY_DASH_MM_DASH_DD_DATE_FORMATTER);
            LOGGER.info("Using user-defined invocationDate {}.", invocationDate);
            return invocationDate;
        }
        LocalDate result = LocalDate.now(Clock.systemDefaultZone());
        LOGGER.info("Using default invocationDate {}.", result);
        return result;
    }

    @Bean
    NamingStrategy namingStrategy() {
        return new DefaultNamingStrategy();
    }

    @Bean
    @Profile("!remote")
    DataResolverRegistry localDataResolverRegistry() {
        LOGGER.info("inputBaseDirectory : {}", inputBaseDirectory);
        LOGGER.info("outputBaseDirectory: {}", outputBaseDirectory);

        MapDataResolverRegistry result = new MapDataResolverRegistry();

        result.registerResolver(DataLocation.INPUT, new DataResolver(new FileDataResolutionStrategy(namingStrategy(), new File(preprocessDirectory(inputBaseDirectory)))));
        result.registerResolver(DataLocation.OUTPUT, new DataResolver(new FileDataResolutionStrategy(namingStrategy(), new File(preprocessDirectory(outputBaseDirectory)))));

        LOGGER.info("Returning default dataResolverRegistry:\n{}", result);
        return result;
    }

    @Bean
    @Profile("remote")
    DataResolverRegistry remoteDataResolverRegistry() {
        LOGGER.info("inputBaseDirectory : {}", inputBaseDirectory);
        LOGGER.info("outputBaseDirectory: {}", outputBaseDirectory);

        MapDataResolverRegistry result = new MapDataResolverRegistry();

        result.registerResolver(DataLocation.INPUT, new DataResolver(new UrlDataResolutionStrategy(namingStrategy(), inputBaseDirectory)));
        result.registerResolver(DataLocation.OUTPUT,  new DataResolver(new UrlDataResolutionStrategy(namingStrategy(), outputBaseDirectory)));

        LOGGER.info("Returning dataResolverRegistry for 'remote' profile:\n{}", result);
        return result;
    }

    @Bean
    @Lazy
    @Profile("remote")
    public SparkSession.Builder sparkSessionBuilder() {

        LOGGER.info("Returning sparkBuilder for 'remote' profile.");

        return SparkSession.builder();
    }

    @Bean
    @Lazy
    @Profile("!remote")
    public SparkSession.Builder localSparkSessionBuilder() {
        SparkSession.Builder sparkBuilder = SparkSession.builder();

        sparkBuilder.master("local[*]").config("spark.driver.host", "localhost");

        LOGGER.info("Returning default sparkBuilder.");

        return sparkBuilder;
    }

    private static String preprocessDirectory(String directory) {
        Objects.requireNonNull(directory, "directory must not be null!");
        if (directory.startsWith("~/")) {
            return System.getProperty("user.home") + directory.substring(1);
        }
        return directory;
    }
}
