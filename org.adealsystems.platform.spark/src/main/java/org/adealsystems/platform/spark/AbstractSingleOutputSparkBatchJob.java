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

package org.adealsystems.platform.spark;

import org.adealsystems.platform.id.DataFormat;
import org.adealsystems.platform.id.DataIdentifier;
import org.adealsystems.platform.id.DataInstance;
import org.adealsystems.platform.id.DataResolver;
import org.adealsystems.platform.process.DataInstanceRegistry;
import org.adealsystems.platform.process.DataLocation;
import org.adealsystems.platform.process.DataResolverRegistry;
import org.adealsystems.platform.process.WriteMode;
import org.adealsystems.platform.process.exceptions.DuplicateInstanceRegistrationException;
import org.adealsystems.platform.process.exceptions.DuplicateUniqueIdentifierException;
import org.adealsystems.platform.process.exceptions.UnregisteredDataIdentifierException;
import org.adealsystems.platform.process.exceptions.UnregisteredDataResolverException;
import org.adealsystems.platform.process.exceptions.UnsupportedDataFormatException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public abstract class AbstractSingleOutputSparkBatchJob implements SparkDataProcessingJob {
    private static final String SEMICOLON = ";";
    private static final String COMMA = ",";
    private static final String PIPE = "|";
    private static final String SUCCESS_INDICATOR = "_SUCCESS";

    private final DataResolverRegistry dataResolverRegistry;
    private final DataLocation outputLocation;
    private final Set<DataIdentifier> outputIdentifiers;
    private final DataInstanceRegistry dataInstanceRegistry = new DataInstanceRegistry();
    private final Map<String, Object> writerOptions = new HashMap<>();
    private final Map<DataIdentifier, String> processingStatus = new HashMap<>();
    private final boolean storeAsSingleFile;
    private final LocalDate invocationDate;
    private final DatasetLogger datasetLogger;

    private WriteMode writeMode;

    private SparkSession sparkSession;
    private JavaSparkContext sparkContext;

    private SparkResultWriterInterceptor resultWriterInterceptor;

    private SparkProcessingReporter processingReporter;

    private Broadcast<LocalDateTime> broadInvocationIdentifier;
    private final Logger logger;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSingleOutputSparkBatchJob.class);

    public AbstractSingleOutputSparkBatchJob(DataResolverRegistry dataResolverRegistry, DataLocation outputLocation, DataIdentifier outputIdentifier, LocalDate invocationDate, boolean storeAsSingleFile) {
        this.invocationDate = Objects.requireNonNull(invocationDate, "invocationDate must not be null!");
        this.dataResolverRegistry = Objects.requireNonNull(dataResolverRegistry, "dataResolverRegistry must not be null!");
        this.outputLocation = Objects.requireNonNull(outputLocation, "outputLocation must not be null!");

        Set<DataIdentifier> outputIds = new HashSet<>(1);
        outputIds.add(Objects.requireNonNull(outputIdentifier, "outputIdentifier must not be null!"));
        this.outputIdentifiers = Collections.unmodifiableSet(outputIds);

        this.storeAsSingleFile = storeAsSingleFile;

        this.logger = LoggerFactory.getLogger(getClass());
        setWriteMode(WriteMode.DATE);
        DatasetLogger.Context dlc = DatasetLogger.newContext();
        dlc.setLogger(logger);
        this.datasetLogger = new DatasetLogger(dlc);
    }

    public AbstractSingleOutputSparkBatchJob(DataResolverRegistry dataResolverRegistry, DataLocation outputLocation, DataIdentifier outputIdentifier, LocalDate invocationDate) {
        this(dataResolverRegistry, outputLocation, outputIdentifier, invocationDate, false);
    }

    protected final void setWriteMode(WriteMode writeMode) {
        this.writeMode = Objects.requireNonNull(writeMode, "writeMode must not be null!");
    }

    @Override
    public WriteMode getWriteMode() {
        return writeMode;
    }

    @Override
    public Map<DataIdentifier, String> getProcessingStatus() {
        if (processingStatus.isEmpty()) {
            return Collections.emptyMap();
        }

        return new HashMap<>(processingStatus);
    }

    @Override
    public void registerProcessingStatus(DataIdentifier dataIdentifier, String status) {
        processingStatus.put(dataIdentifier, status);
    }

    public void setResultWriterInterceptor(SparkResultWriterInterceptor resultWriterInterceptor) {
        this.resultWriterInterceptor = resultWriterInterceptor;
    }

    public void setProcessingReporter(SparkProcessingReporter processingReporter) {
        this.processingReporter = processingReporter;
    }

    @Override
    public void init(SparkSession sparkSession) {
        Objects.requireNonNull(sparkSession, "sparkSession must not be null!");
        if (this.sparkSession != null) {
            throw new IllegalStateException("sparkSession was already initialized! " + sparkSession);
        }
        this.sparkSession = sparkSession;
        this.sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        registerUdfs(this.sparkSession.udf());
        dataInstanceRegistry.clear(); // get rid of inputs from previous run
        registerInputs();
    }

    @Override
    public Map<String, Object> getWriterOptions() {
        return new HashMap<>(writerOptions);
    }

    @Override
    public void setWriterOptions(Map<String, Object> options) {
        Objects.requireNonNull(options, "options must not be null!");
        writerOptions.putAll(options);
    }

    @Override
    public void setWriterOption(String name, Object value) {
        Objects.requireNonNull(name, "option name must not be null!");
        Objects.requireNonNull(value, "option value must not be null!");
        writerOptions.put(name, value);
    }

    protected LocalDateTime getInvocationIdentifier() {
        return broadInvocationIdentifier.getValue();
    }

    protected DatasetLogger getDatasetLogger() {
        return datasetLogger;
    }

    protected LocalDate getInvocationDate() {
        return invocationDate;
    }

    /**
     * Register your UDFs with the given UDFRegistration.
     *
     * @param udfRegistration the UDFRegistration to be used for registration.
     */
    protected abstract void registerUdfs(UDFRegistration udfRegistration);

    /**
     * This method must register all inputs for the already defined outputIdentifier.
     * <p>
     * Inputs may differ if outputIdentifier contains a configuration.
     *
     * @see #getOutputIdentifiers() method to access the output DataIdentifier.
     */
    protected abstract void registerInputs();

    /**
     * Do the actual work of the batch job in this method and return the
     * result of the job.
     *
     * @return the result of the job
     */
    protected abstract Dataset<Row> processData();

    @Override
    public void execute() {
        long startTime = System.currentTimeMillis();
        LocalDateTime timestamp = LocalDateTime.now(Clock.systemDefaultZone());

        try {
            broadInvocationIdentifier = getSparkContext().broadcast(timestamp);
            writeOutput(processData());

            if (processingReporter != null) {
                long stopTime = System.currentTimeMillis();
                long duration = stopTime - startTime;

                processingReporter.reportSuccess(this, timestamp, duration);
            }
        } catch (Throwable th) {
            if (processingReporter != null) {
                long stopTime = System.currentTimeMillis();
                long duration = stopTime - startTime;

                processingReporter.reportFailure(this, timestamp, duration, th);
            }

            for (DataIdentifier dataId : getOutputIdentifiers()) {
                registerProcessingStatus(dataId, "processing-error");
            }

            throw th;
        }
    }

    @Override
    public Set<DataIdentifier> getOutputIdentifiers() {
        return outputIdentifiers;
    }

    @Override
    @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
    public Map<DataIdentifier, Set<DataInstance>> getInputInstances() {
        Set<DataIdentifier> identifiers = dataInstanceRegistry.allIdentifier();

        Map<DataIdentifier, Set<DataInstance>> result = new TreeMap<>();
        for (DataIdentifier current : identifiers) {
            result.put(current, new TreeSet<>(dataInstanceRegistry.resolveAll(current)));
        }

        return result;
    }

    @Override
    public void close() {
        if (sparkContext != null) {
            logger.debug("Closing JavaSparkContext...");
            sparkContext.close();
            sparkContext = null;
            sparkSession = null;
        }
    }

    /**
     * Registers the "current" DataInstance as an input for the given location and identifier, i.e. the single one
     * that does not include a date.
     *
     * @param dataLocation    the DataLocation
     * @param inputIdentifier the DataIdentifier of the input
     * @throws NullPointerException                   if either dataLocation or inputIdentifier are null
     * @throws UnregisteredDataResolverException      if no DataResolver has been registered for the given DataLocation
     * @throws DuplicateInstanceRegistrationException if the data instance was already registered
     */
    protected void registerCurrentInput(DataLocation dataLocation, DataIdentifier inputIdentifier) {
        Objects.requireNonNull(dataLocation, "dataLocation must not be null!");
        Objects.requireNonNull(inputIdentifier, "inputIdentifier must not be null!");

        DataResolver inputDataResolver = dataResolverRegistry.getResolverFor(dataLocation);

        dataInstanceRegistry.register(inputDataResolver.createCurrentInstance(inputIdentifier));
    }

    /**
     * Registers an input for the given location, identifier and date.
     *
     * @param dataLocation    the DataLocation
     * @param inputIdentifier the DataIdentifier of the input
     * @param date            the date to be associated with the data instance
     * @throws NullPointerException                   if either dataLocation, inputIdentifier or date are null
     * @throws UnregisteredDataResolverException      if no DataResolver has been registered for the given DataLocation
     * @throws DuplicateInstanceRegistrationException if the data instance was already registered
     */
    protected void registerInput(DataLocation dataLocation, DataIdentifier inputIdentifier, LocalDate date) {
        Objects.requireNonNull(dataLocation, "dataLocation must not be null!");
        Objects.requireNonNull(inputIdentifier, "inputIdentifier must not be null!");
        Objects.requireNonNull(date, "date must not be null!");

        DataResolver inputDataResolver = dataResolverRegistry.getResolverFor(dataLocation);

        dataInstanceRegistry.register(inputDataResolver.createDateInstance(inputIdentifier, date));
    }

    /**
     * Reads the Dataset registered for the given data identifier.
     * <p>
     * This method throws an exception if more than one DataInstance was
     * registered for the given DataIdentifier or if no DataIdentifier was
     * registered for the given DataIdentifier at all.
     * <p>
     * Any exception happening during reading of the Dataset is propagated to the caller.
     *
     * @param dataIdentifier the data identifier used to resolve the data instance
     * @return the (unique) Dataset for the given DataIdentifier
     * @throws NullPointerException                if dataIdentifier is null
     * @throws DuplicateUniqueIdentifierException  if more than one DataInstance was registered for the given DataIdentifier
     * @throws UnregisteredDataIdentifierException if no DataInstance was registered for the given DataIdentifier
     */
    protected Dataset<Row> readInput(DataIdentifier dataIdentifier) {
        DataInstance dataInstance = dataInstanceRegistry.resolveUnique(dataIdentifier).orElse(null);
        if (dataInstance != null) {
            return readInput(dataInstance);
        }
        throw new UnregisteredDataIdentifierException(dataIdentifier);
    }

    /**
     * Reads the Dataset registered for the given data identifier.
     * <p>
     * This method throws an exception if more than one DataInstance was
     * registered for the given DataIdentifier or if no DataIdentifier was
     * registered for the given DataIdentifier at all.
     * <p>
     * Any exception happening during reading of the Dataset is silently ignored
     * and Optional.empty() is returned instead.
     *
     * @param dataIdentifier the data identifier used to resolve the data instance
     * @return the (unique) Dataset for the given DataIdentifier
     * @throws NullPointerException                if dataIdentifier is null
     * @throws DuplicateUniqueIdentifierException  if more than one DataInstance was registered for the given DataIdentifier
     * @throws UnregisteredDataIdentifierException if no DataInstance was registered for the given DataIdentifier
     */
    protected Optional<Dataset<Row>> readOptionalInput(DataIdentifier dataIdentifier) {
        Objects.requireNonNull(dataIdentifier, "dataIdentifier must not be null!");

        DataInstance dataInstance = dataInstanceRegistry.resolveUnique(dataIdentifier).orElse(null);

        if (dataInstance != null) {
            try {
                return Optional.of(readInput(dataIdentifier));
            } catch (Throwable t) {
                logger.debug("Reading {} failed. Returning Optional.empty().", dataIdentifier, t);
                return Optional.empty();
            }
        }
        throw new UnregisteredDataIdentifierException(dataIdentifier);
    }

    /**
     * Returns all datasets for all data instances registered for the given data identifier.
     * <p>
     * This method throws an exception if any registered DataInstance fails to load.
     *
     * @param dataIdentifier the data identifier used to resolve the data instances
     * @return a (sorted) map with DataIdentifier as key and Dataset as value.
     * @throws NullPointerException                if dataIdentifier is null
     * @throws UnregisteredDataIdentifierException if no DataInstance was registered for the given DataIdentifier
     */
    protected Map<DataInstance, Dataset<Row>> readAll(DataIdentifier dataIdentifier) {
        Set<DataInstance> instances = dataInstanceRegistry.resolveAll(dataIdentifier);

        if (instances.isEmpty()) {
            throw new UnregisteredDataIdentifierException(dataIdentifier);
        }

        Map<DataInstance, Dataset<Row>> result = new TreeMap<>();
        for (DataInstance current : instances) {
            result.put(current, readInput(current));
        }

        return result;
    }

    /**
     * Returns all datasets for all data instances registered for the given data identifier.
     * <p>
     * This method doesn't throw an exception if any registered DataInstance fails to load.
     * They simply won't be included in the result map instead.
     *
     * @param dataIdentifier the data identifier used to resolve the data instances
     * @return a (sorted) map with DataIdentifier as key and Dataset as value.
     * @throws NullPointerException                if dataIdentifier is null
     * @throws UnregisteredDataIdentifierException if no DataInstance was registered for the given DataIdentifier
     */
    protected Map<DataInstance, Dataset<Row>> readAllAvailable(DataIdentifier dataIdentifier) {
        Set<DataInstance> instances = dataInstanceRegistry.resolveAll(dataIdentifier);

        if (instances.isEmpty()) {
            throw new UnregisteredDataIdentifierException(dataIdentifier);
        }

        Map<DataInstance, Dataset<Row>> result = new TreeMap<>();
        for (DataInstance current : instances) {
            try {
                result.put(current, readInput(current));
            } catch (Throwable t) {
                logger.debug("Reading {} failed.", current, t);
            }
        }

        return result;
    }

    private Dataset<Row> readInput(DataInstance dataInstance) {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        String path = dataInstance.getPath();
        DataFormat dataFormat = dataInstance.getDataFormat();
        logger.info("Reading {} from '{}'.", dataInstance, path);
        switch (dataFormat) {
            case CSV_COMMA:
                return readCsvAsDataset(COMMA, path);
            case CSV_SEMICOLON:
                return readCsvAsDataset(SEMICOLON, path);
            case CSV_PIPE:
                return readCsvAsDataset(PIPE, path);
            case JSON:
                return readJsonAsDataset(path);
            case AVRO:
                return readAvroAsDataset(path);
            default:
                throw new UnsupportedDataFormatException(dataFormat);
        }
    }

    @Override
    public DataResolver getOutputDataResolver() {
        return dataResolverRegistry.getResolverFor(outputLocation);
    }

    private void writeOutput(Dataset<Row> outputDataset) {
        Objects.requireNonNull(outputDataset, "outputDataset must not be null!");

        if (outputIdentifiers.size() != 1) {
            throw new IllegalArgumentException("More than one output identifier configured! Please write your output with specifying a corresponding identifier!");
        }
        // use the only one configured output identifier
        DataIdentifier outputIdentifier = outputIdentifiers.iterator().next();

        DataResolver dataResolver = getOutputDataResolver();

        getDatasetLogger().showInfo("About to write the following data for " + outputIdentifier, outputDataset);
        outputDataset = outputDataset.repartition(1); // always repartition(1) before writing!
        if (writeMode == WriteMode.DATE || writeMode == WriteMode.BOTH) {
            DataInstance dateInstance = dataResolver.createDateInstance(outputIdentifier, invocationDate);

            if (resultWriterInterceptor != null) {
                logger.info("Registering result of {} by sparkResultWriterInterceptor.", dateInstance);
                resultWriterInterceptor.registerResult(outputLocation, dateInstance, outputDataset);
            }

            writeOutput(dateInstance, outputDataset);
        }

        if (writeMode == WriteMode.CURRENT || writeMode == WriteMode.BOTH) {
            DataInstance currentInstance = dataResolver.createCurrentInstance(outputIdentifier);
            writeOutput(currentInstance, outputDataset);
        }
    }

    private void writeOutput(DataInstance dataInstance, Dataset<Row> result) {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        String path = dataInstance.getPath();
        logger.info("Writing {} to '{}'.", dataInstance, path);
        DataFormat dataFormat = dataInstance.getDataFormat();
        switch (dataFormat) {
            case CSV_COMMA:
                writeDatasetAsCsv(COMMA, result, path, storeAsSingleFile, sparkContext, writerOptions);
                return;
            case CSV_SEMICOLON:
                writeDatasetAsCsv(SEMICOLON, result, path, storeAsSingleFile, sparkContext, writerOptions);
                return;
            case CSV_PIPE:
                writeDatasetAsCsv(PIPE, result, path, storeAsSingleFile, sparkContext, writerOptions);
                return;
            case JSON:
                writeDatasetAsJson(result, path, storeAsSingleFile, sparkContext, writerOptions);
                return;
            case AVRO:
                writeDatasetAsAvro(result, path, storeAsSingleFile, sparkContext, writerOptions);
                return;
            default:
                throw new UnsupportedDataFormatException(dataFormat);
        }
    }

    protected SparkSession getSparkSession() {
        if (sparkSession == null) {
            throw new IllegalStateException("sparkSession has not been initialized!");
        }
        return sparkSession;
    }

    protected JavaSparkContext getSparkContext() {
        if (sparkContext == null) {
            throw new IllegalStateException("sparkContext has not been initialized!");
        }
        return sparkContext;
    }

    private Dataset<Row> readCsvAsDataset(String delimiter, String fileName) {
        // TODO: option names
        // https://spark.apache.org/docs/2.4.3/api/java/org/apache/spark/sql/DataFrameWriter.html#csv-java.lang.String-
        return getSparkSession().read() //
            .option("header", "true") //
            .option("inferSchema", "true") //
            .option("delimiter", delimiter) // TODO: option names
            .option("emptyValue", "") // TODO: option names
            .csv(fileName);
    }

    private Dataset<Row> readJsonAsDataset(String fileName) {
        return getSparkSession().read() //
            .json(fileName);
    }

    private Dataset<Row> readAvroAsDataset(String fileName) {
        return getSparkSession().read() //
            .format("avro") //
            .load(fileName);
    }

    private static void writeDatasetAsCsv(
        String delimiter,
        Dataset<Row> dataset,
        String fileName,
        boolean storeAsSingleFile,
        JavaSparkContext sparkContext,
        Map<String, Object> writerOptions
    ) {
        String targetPath = fileName;
        if (storeAsSingleFile) {
            targetPath = fileName + "__temp";
        }

        // https://spark.apache.org/docs/2.4.3/api/java/org/apache/spark/sql/DataFrameWriter.html#csv-java.lang.String-
        DataFrameWriter<Row> writer = dataset.write()
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .option("delimiter", delimiter) // TODO: option names
            .option("emptyValue", ""); // TODO: option names

        for (Map.Entry<String, Object> option : writerOptions.entrySet()) {
            Object value = option.getValue();
            Class<?> valueClass = value.getClass();
            if (Long.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (long) value);
            } else if (Double.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (double) value);
            } else if (Boolean.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (boolean) value);
            } else {
                writer.option(option.getKey(), (String) value);
            }
        }

        writer.csv(targetPath);

        if (storeAsSingleFile) {
            moveFile(sparkContext, targetPath, fileName);
        }
    }

    private static void writeDatasetAsJson(Dataset<Row> dataset, String fileName, boolean storeAsSingleFile, JavaSparkContext sparkContext, Map<String, Object> writerOptions) {
        String targetPath = fileName;
        if (storeAsSingleFile) {
            targetPath = fileName + "__temp";
        }

        DataFrameWriter<Row> writer = dataset.write()
            .mode(SaveMode.Overwrite);

        for (Map.Entry<String, Object> option : writerOptions.entrySet()) {
            Object value = option.getValue();
            Class<?> valueClass = value.getClass();
            if (Long.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (long) value);
            } else if (Double.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (double) value);
            } else if (Boolean.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (boolean) value);
            } else {
                writer.option(option.getKey(), (String) value);
            }
        }

        writer.json(targetPath);

        if (storeAsSingleFile) {
            moveFile(sparkContext, targetPath, fileName);
        }
    }

    private static void writeDatasetAsAvro(Dataset<Row> dataset, String fileName, boolean storeAsSingleFile, JavaSparkContext sparkContext, Map<String, Object> writerOptions) {
        String targetPath = fileName;
        if (storeAsSingleFile) {
            targetPath = fileName + "__temp";
        }

        DataFrameWriter<Row> writer = dataset.write()
            .mode(SaveMode.Overwrite)
            .format("avro");

        for (Map.Entry<String, Object> option : writerOptions.entrySet()) {
            Object value = option.getValue();
            Class<?> valueClass = value.getClass();
            if (Long.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (long) value);
            } else if (Double.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (double) value);
            } else if (Boolean.class.isAssignableFrom(valueClass)) {
                writer.option(option.getKey(), (boolean) value);
            } else {
                writer.option(option.getKey(), (String) value);
            }
        }

        writer.save(targetPath);

        if (storeAsSingleFile) {
            moveFile(sparkContext, targetPath, fileName);
        }
    }

    // this is broken code... but now it's in a single place
    static void moveFile(JavaSparkContext sparkContext, String source, String target) {
        org.apache.hadoop.conf.Configuration hadoopConfig = sparkContext.hadoopConfiguration();
        hadoopConfig.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
        hadoopConfig.setBoolean("fs.s3a.impl.disable.cache", true);
        hadoopConfig.setBoolean("fs.s3n.impl.disable.cache", true);
        hadoopConfig.setBoolean("fs.s3.impl.disable.cache", true);
        Path sourceFSPath = new Path(source);
        try (FileSystem fs = sourceFSPath.getFileSystem(hadoopConfig)) {
            String successFilename = source + "/" + SUCCESS_INDICATOR;
            LOGGER.warn("successFilename: {}", successFilename);
            Path successPath = new Path(successFilename);
            LOGGER.warn("successPath: {}", successPath);

            while (!fs.exists(successPath)) {
                try {
                    LOGGER.warn("Waiting for appearance of {}...", successFilename);
                    Thread.sleep(1000);
                    // this is an endless loop
                    // and there isn't something like a _FAILURE indicator, afaik
                    // not ideal
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Exception while waiting for success file!", e);
                }
            }
            LOGGER.warn("Found successPath");
            FileStatus[] globs = fs.globStatus(new Path(source + "/part-*"));
            if (globs == null) {
                // from Globber.doGlob():
                /*
                 * When the input pattern "looks" like just a simple filename, and we
                 * can't find it, we return null rather than an empty array.
                 * This is a special case which the shell relies on.
                 *
                 * To be more precise: if there were no results, AND there were no
                 * groupings (aka brackets), and no wildcards in the input (aka stars),
                 * we return null.
                 */
                throw new IllegalStateException("globStatus() returned null! This should not happen...");
            }
            LOGGER.warn("Globs: {}", (Object) globs);
            if (globs.length != 1) {
                LOGGER.error("Expected one part but found {}! {}", globs.length, globs);
                throw new IllegalStateException("Expected one part but found " + globs.length);
            }
            Path sourcePath = globs[0].getPath();
            Path targetPath = new Path(target);
            LOGGER.warn("About to rename sourcePath: {} to targetPath: {}", sourcePath, targetPath);
            if (fs.rename(sourcePath, targetPath)) {
                if (!fs.delete(new Path(source), true)) {
                    LOGGER.warn("Failed to delete source: {}", source);
                }
            } else {
                LOGGER.warn("Failed to rename sourcePath: {} to targetPath: {}", sourcePath, targetPath);
                LOGGER.warn("About to copy sourcePath: {} to targetPath: {}", sourcePath, targetPath);
                if (!FileUtil.copy(fs, sourcePath, fs, targetPath, true, hadoopConfig)) {
                    LOGGER.warn("Failed to copy sourcePath: {} to targetPath: {}", sourcePath, targetPath);
                } else {
                    if (!fs.delete(new Path(source), true)) {
                        LOGGER.warn("Failed to delete source: {}", source);
                    }
                }
            }

        } catch (IOException ex) {
            throw new IllegalStateException("Unable to rename result file!", ex);
        }
    }
}
