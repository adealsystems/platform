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

package org.adealsystems.platform.spark.test.junit5;

import org.adealsystems.platform.DataLocation;
import org.adealsystems.platform.DataResolver;
import org.adealsystems.platform.DataResolverRegistry;
import org.adealsystems.platform.DefaultNamingStrategy;
import org.adealsystems.platform.MapDataResolverRegistry;
import org.adealsystems.platform.file.FileDataResolutionStrategy;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
public abstract class AbstractBatchJobTest {

    protected static SparkSession sparkSession;
    protected static DataResolverRegistry dataResolverRegistry;

    @BeforeAll
    public static void beforeClass() throws IOException {
        Path baseDirectory = Files.createTempDirectory("batch-spec");
        sparkSession = SparkSession.builder().master("local[*]").appName("TEST").getOrCreate();
        DataResolver dataResolver = new DataResolver(new FileDataResolutionStrategy(new DefaultNamingStrategy(), baseDirectory.toFile()));

        MapDataResolverRegistry registry = new MapDataResolverRegistry();
        registry.registerResolver(DataLocation.INPUT, dataResolver);
        registry.registerResolver(DataLocation.OUTPUT, dataResolver);
        dataResolverRegistry = registry;
    }
}