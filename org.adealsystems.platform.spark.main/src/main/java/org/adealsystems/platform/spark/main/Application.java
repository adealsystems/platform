/*
 * Copyright 2020-2022 ADEAL Systems GmbH
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

import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.spark.SparkDataProcessingJob;
import org.adealsystems.platform.time.TimeHandling;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.adealsystems.platform.spark.main.SystemProperties.loadPropertiesFrom;
import static org.adealsystems.platform.spark.main.SystemProperties.replaceProperty;

@SuppressWarnings("PMD.UseUtilityClass")
@ComponentScan({"org.adealsystems.platform.spark.config", "org.adealsystems.platform.spark.main"})
@PropertySource("classpath:adeal-platform-git.properties")
@PropertySource("classpath:application.properties")
@PropertySource(value = "file:application.properties", ignoreResourceNotFound = true)
public class Application {
    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final String JOB_PREFIX = "--job=";
    private static final String INVOCATION_DATE_PREFIX = "--invocation-date=";
    private static final String REMOTE = "--remote";
    private static final String DEBUG = "--debug";
    private static final String SPRING_PROFILES_ACTIVE_PROPERTY_NAME = "spring.profiles.active";
    private static final String PROFILE_REPLACEMENT_PROPERTIES_PATH = "profile-replacement.properties";

    public static void main(String[] args) throws IOException {
        poorMansBootBanner();
        poorMansSpringProfileInclude();
        ParsedArgs parsedArgs = processArgs(args);

        if (parsedArgs.isDebug()) {
            debugProperties();
            debugResource("log4j.xml");
            debugResource("application.properties");
            debugResource("adeal-platform-git.properties");
        }

        //String[] remainingArgs = parsedArgs.getRemaining().toArray(new String[]{});
        DataIdentifier job = parsedArgs.getJob();

        try (ConfigurableApplicationContext applicationContext = new AnnotationConfigApplicationContext(Application.class)) {

            Map<String, SparkDataProcessingJob> beans = applicationContext.getBeansOfType(SparkDataProcessingJob.class);
            Map<DataIdentifier, SparkDataProcessingJob> jobs = processJobs(beans);
            if (jobs.isEmpty()) {
                LOGGER.warn("Could not find any jobs of type SparkDataProcessingJob!");
                return;
            }

            if (jobs.size() == 1 && job == null) {
                job = jobs.keySet().stream().findFirst().get();
            }

            boolean unrecognized = false;
            if (job == null) {
                LOGGER.warn("No job specified with --job=output_identifier!");
                unrecognized = true;
            } else {
                if (!jobs.containsKey(job)) {
                    LOGGER.warn("Unrecognized job: {}", job);
                    unrecognized = true;
                }
            }

            if (unrecognized) {
                Set<DataIdentifier> validJobs = new TreeSet<>(jobs.keySet());
                StringBuilder builder = new StringBuilder();
                for (DataIdentifier currentJob : validJobs) {
                    if (builder.length() != 0) {
                        builder.append('\n');
                    }
                    builder.append("--job=").append(currentJob);
                }
                LOGGER.warn("\nValid jobs:\n{}", builder);
                return;
            }
            SparkSession.Builder sparkSessionBuilder = applicationContext.getBean(SparkSession.Builder.class)
                .appName("ADEAL-Systems-Batch");
            SparkDataProcessingJob currentJob = null; // NOPMD
            try (SparkSession sparkSession = sparkSessionBuilder.getOrCreate()) {
                try (SparkDataProcessingJob sparkJob = jobs.get(job)) {
                    currentJob = sparkJob;
                    processJob(sparkJob, sparkSession);
                } catch (Exception ex) {
                    LOGGER.warn("Exception while processing {}!", job, ex);
                }
            }

            // finalizing the job
            if (currentJob != null) {
                currentJob.finalizeJob();
            }
        }
    }

    @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops", "PMD.SystemPrintln"})
    private static void debugProperties() {
        Properties props = System.getProperties();
        Map<String, String> foo = new TreeMap<>();
        String pathSeparator = props.getProperty("path.separator");
        for (Map.Entry<Object, Object> current : props.entrySet()) {
            foo.put(String.valueOf(current.getKey()), String.valueOf(current.getValue()));
        }
        StringBuilder builder = new StringBuilder("System.getProperties():");

        Set<String> paths = new HashSet<>();
        paths.add("java.class.path");
        paths.add("java.endorsed.dirs");
        paths.add("java.ext.dirs");
        paths.add("java.library.path");
        paths.add("sun.boot.class.path");
        paths.add("sun.boot.library.path");
        paths.add("spark.driver.extraClassPath");
        paths.add("spark.executor.extraClassPath");

        for (Map.Entry<String, String> current : foo.entrySet()) {
            String key = current.getKey();
            String value = current.getValue();

            if ("line.separator".equals(key)) {
                value = value.replace("\r", "\\r");
                value = value.replace("\n", "\\n");
            } else if (paths.contains(key)) {
                StringTokenizer tok = new StringTokenizer(value, pathSeparator);
                Set<String> entries = new TreeSet<>();
                while (tok.hasMoreTokens()) {
                    entries.add(tok.nextToken());
                }
                StringBuilder cpBuilder = new StringBuilder();
                for (String entry : entries) {
                    if (cpBuilder.length() != 0) {
                        cpBuilder.append("\n\t");
                    }
                    cpBuilder.append("- ").append(entry);
                }
                value = cpBuilder.toString();
            }

            builder.append("\n- ").append(key).append(":\n\t").append(value);
        }
        System.err.println(builder);

    }

    @SuppressWarnings({"PMD.SystemPrintln", "PMD.AvoidPrintStackTrace"})
    private static void debugResource(String path) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            Set<String> found = new TreeSet<>();
            Enumeration<URL> resources = classLoader.getResources(path);
            while (resources.hasMoreElements()) {
                found.add(String.valueOf(resources.nextElement()));
            }
            StringBuilder builder = new StringBuilder(200);
            int count = found.size();
            builder.append("\nFound ").append(count).append(" resource");
            if (count > 1) {
                builder.append('s');
            }
            builder.append(" for \"").append(path).append('"');
            if (count == 0) {
                builder.append('.');
            } else {
                builder.append(':');
                for (String current : found) {
                    builder.append("\n- ").append(current);
                }
            }
            System.err.println(builder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void processJob(SparkDataProcessingJob sparkJob, SparkSession sparkSession) {
        if (LOGGER.isInfoEnabled())
            LOGGER.info("\n\n## Starting batch job {}...\n#### Class : {}", sparkJob.getOutputIdentifiers(), sparkJob.getClass());
        sparkJob.init(sparkSession); // no, sparkSession.newSession() does not help. m(
        if (LOGGER.isInfoEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("\n###### Inputs:");
            sparkJob.getInputInstances().keySet().stream()
                .map(DataIdentifier::toString)
                .sorted()
                .forEach(it -> builder.append("\n###### - ").append(it));

            LOGGER.info(builder.toString());
        }
        sparkJob.execute();
        if (LOGGER.isInfoEnabled()) LOGGER.info("\n## Finished batch job {}", sparkJob.getOutputIdentifiers());
    }

    private static ParsedArgs processArgs(String[] args) {
        // TODO: use something else to parse arguments
        // the problem is that most tooling expects arguments to
        // be parsed from start to finish.
        // We, on the other hand, would like to parse *some* arguments
        // right here while propagating the remaining "unused" arguments
        // to spring-boot...
        List<String> remaining = new ArrayList<>();
        DataIdentifier job = null;
        boolean debug = false;
        for (String current : args) {
            if (REMOTE.equals(current)) {
                System.setProperty("spring.profiles.active", "remote");
                continue;
            }
            if (DEBUG.equals(current)) {
                debug = true;
                continue;
            }
            if (current.startsWith(JOB_PREFIX)) {
                String jobString = current.substring(JOB_PREFIX.length());
                job = DataIdentifier.fromString(jobString);
                continue;
            }
            if (current.startsWith(INVOCATION_DATE_PREFIX)) {
                String dateString = current.substring(INVOCATION_DATE_PREFIX.length());
                LocalDate parsed = LocalDate.parse(dateString, TimeHandling.YYYY_DASH_MM_DASH_DD_DATE_FORMATTER);
                System.setProperty("invocation.date", parsed.toString());
                continue;
            }
            LOGGER.warn("Remaining: {}", current);
            remaining.add(current);
        }

        return new ParsedArgs(job, debug, remaining);

    }

    @SuppressWarnings("PMD.CloseResource")
    private static Map<DataIdentifier, SparkDataProcessingJob> processJobs(Map<String, SparkDataProcessingJob> jobs) {
        Objects.requireNonNull(jobs, "jobs must not be null!");

        Map<DataIdentifier, SparkDataProcessingJob> result = new HashMap<>();
        for (Map.Entry<String, SparkDataProcessingJob> entry : jobs.entrySet()) {
            SparkDataProcessingJob value = entry.getValue();
            Set<DataIdentifier> outputIdentifiers = value.getOutputIdentifiers();
            if (outputIdentifiers == null || outputIdentifiers.isEmpty()) {
                LOGGER.warn("Found a processing job {} without any output data identifier configured!", value);
                continue;
            }

            for (DataIdentifier outputIdentifier : outputIdentifiers) {
                SparkDataProcessingJob previous = result.put(outputIdentifier, value);
                if (previous != null) {
                    throw new IllegalStateException("Duplicate entries for " + outputIdentifier + "! previous: " + previous.getClass() + ",  current: " + value.getClass());
                }
            }
        }
        return result;
    }

    private static void poorMansSpringProfileInclude() {
        replaceProperty(SPRING_PROFILES_ACTIVE_PROPERTY_NAME, PROFILE_REPLACEMENT_PROPERTIES_PATH);
    }


    @SuppressWarnings("PMD.SystemPrintln")
    private static void poorMansBootBanner() throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        String text;
        try (InputStream is = cl.getResourceAsStream("banner.txt")) {
            if (is == null) {
                throw new IllegalStateException("Failed to load banner.txt!");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                text = reader.lines().collect(Collectors.joining("\n"));
            }
        }

        Properties bannerProperties = createBannerProperties();
        for (String key : bannerProperties.stringPropertyNames()) {
            String value = bannerProperties.getProperty(key);
            if (value == null) {
                continue;
            }
            text = text.replace("${" + key + "}", value);
        }

        System.out.println(text);
    }

    private static Properties createBannerProperties() throws IOException {
        Properties props = new Properties();

        loadPropertiesFrom(props, "adeal-platform-git.properties");

        boolean dirty = Boolean.parseBoolean(props.getProperty("git.dirty"));
        props.setProperty("git.dirty.text", dirty ? " (dirty)" : "");

        LOGGER.debug("Default-Properties: {}", props);
        return props;
    }


    private static class ParsedArgs {
        private final DataIdentifier job;
        private final List<String> remaining;
        private final boolean debug;

        ParsedArgs(DataIdentifier job, boolean debug, List<String> remaining) {
            this.job = job;
            this.remaining = remaining;
            this.debug = debug;
        }

        public DataIdentifier getJob() {
            return job;
        }

        public List<String> getRemaining() {
            return remaining;
        }

        public boolean isDebug() {
            return debug;
        }
    }
}
