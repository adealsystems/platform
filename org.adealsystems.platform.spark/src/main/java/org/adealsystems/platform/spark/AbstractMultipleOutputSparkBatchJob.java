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

import org.adealsystems.platform.DataFormat;
import org.adealsystems.platform.DataIdentifier;
import org.adealsystems.platform.DataInstance;
import org.adealsystems.platform.DataInstanceRegistry;
import org.adealsystems.platform.DataLocation;
import org.adealsystems.platform.DataResolver;
import org.adealsystems.platform.DataResolverRegistry;
import org.adealsystems.platform.exceptions.DuplicateInstanceRegistrationException;
import org.adealsystems.platform.exceptions.DuplicateUniqueIdentifierException;
import org.adealsystems.platform.exceptions.UnregisteredDataIdentifierException;
import org.adealsystems.platform.exceptions.UnregisteredDataResolverException;
import org.adealsystems.platform.exceptions.UnsupportedDataFormatException;
import org.apache.hadoop.conf.Configuration;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public abstract class AbstractMultipleOutputSparkBatchJob implements SparkDataProcessingJob {
    private static final String SEMICOLON = ";";
    private static final String COMMA = ",";
    private static final String PIPE = "|";

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
    private SparkDataIdentifierStatusUpdater statusUpdater;

    private Broadcast<LocalDateTime> broadInvocationIdentifier;
    private final Logger logger;

    public AbstractMultipleOutputSparkBatchJob(DataResolverRegistry dataResolverRegistry, DataLocation outputLocation, Collection<DataIdentifier> outputIdentifiers, LocalDate invocationDate, boolean storeAsSingleFile) {
        this.invocationDate = Objects.requireNonNull(invocationDate, "invocationDate must not be null!");
        this.dataResolverRegistry = Objects.requireNonNull(dataResolverRegistry, "dataResolverRegistry must not be null!");
        this.outputLocation = Objects.requireNonNull(outputLocation, "outputLocation must not be null!");

        Set<DataIdentifier> outputIds = new HashSet<>(Objects.requireNonNull(outputIdentifiers, "outputIdentifiers must not be null!"));
        this.outputIdentifiers = Collections.unmodifiableSet(outputIds);

        this.storeAsSingleFile = storeAsSingleFile;

        this.logger = LoggerFactory.getLogger(getClass());
        setWriteMode(WriteMode.DATE);
        DatasetLogger.Context dlc = DatasetLogger.newContext();
        dlc.setLogger(logger);
        this.datasetLogger = new DatasetLogger(dlc);
    }

    public AbstractMultipleOutputSparkBatchJob(DataResolverRegistry dataResolverRegistry, DataLocation outputLocation, Collection<DataIdentifier> outputIdentifiers, LocalDate invocationDate) {
        this(dataResolverRegistry, outputLocation, outputIdentifiers, invocationDate, false);
    }

    public void setResultWriterInterceptor(SparkResultWriterInterceptor sparkResultWriterInterceptor) {
        this.resultWriterInterceptor = sparkResultWriterInterceptor;
    }

    public void setProcessingReporter(SparkProcessingReporter processingReporter) {
        this.processingReporter = processingReporter;
    }

    public void setStatusUpdater(SparkDataIdentifierStatusUpdater statusUpdater) {
        this.statusUpdater = statusUpdater;
    }

    protected final void setWriteMode(WriteMode writeMode) {
        this.writeMode = Objects.requireNonNull(writeMode, "writeMode must not be null!");
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
    protected abstract Map<DataIdentifier, Dataset<Row>> processData();

    @Override
    public void execute() {
        long startTime = System.currentTimeMillis();
        LocalDateTime timestamp = LocalDateTime.now(Clock.systemDefaultZone());

        try {
            broadInvocationIdentifier = getSparkContext().broadcast(timestamp);

            Map<DataIdentifier, Dataset<Row>> results = processData();
            for (Map.Entry<DataIdentifier, Dataset<Row>> entry : results.entrySet()) {
                DataIdentifier outputIdentifier = entry.getKey();
                Dataset<Row> dataset = entry.getValue();
                writeOutput(outputIdentifier, dataset);
            }

            if (processingReporter != null) {
                long stopTime = System.currentTimeMillis();
                long duration = stopTime - startTime;

                processingReporter.reportSuccess(this, timestamp, duration);
            }
        }
        catch(Throwable th) {
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
        finally {
            if (statusUpdater != null) {
                for (Map.Entry<DataIdentifier, String> entry : getProcessingStatus().entrySet()) {
                    DataIdentifier dataId = entry.getKey();
                    String status = entry.getValue();
                    statusUpdater.updateStatus(dataId, status);
                }
            }
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
     * Registers inputs for the given location, identifier and date.
     *
     * @param dataLocation     the DataLocation
     * @param inputIdentifiers DataIdentifier of the input
     * @param date             the date to be associated with the data instance
     * @throws NullPointerException                   if either dataLocation, inputIdentifier or date are null
     * @throws UnregisteredDataResolverException      if no DataResolver has been registered for the given DataLocation
     * @throws DuplicateInstanceRegistrationException if the data instance was already registered
     */
    protected void registerInputs(DataLocation dataLocation, Collection<DataIdentifier> inputIdentifiers, LocalDate date) {
        Objects.requireNonNull(dataLocation, "dataLocation must not be null!");
        Objects.requireNonNull(inputIdentifiers, "inputIdentifiers must not be null!");
        Objects.requireNonNull(date, "date must not be null!");

        DataResolver inputDataResolver = dataResolverRegistry.getResolverFor(dataLocation);

        for (DataIdentifier inputIdentifier : inputIdentifiers) {
            dataInstanceRegistry.register(inputDataResolver.createDateInstance(inputIdentifier, date));
        }
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
     * Reads Datasets registered for the given data identifier and combines all of them to one Dataset.
     * <p>
     * This method throws an exception if more than one DataInstance was
     * registered for the given DataIdentifier or if no DataIdentifier was
     * registered for the given DataIdentifier at all.
     * <p>
     * Any exception happening during reading of the Dataset is propagated to the caller.
     *
     * @param dataIdentifiers data identifiers used to resolve the data instance
     * @return the (unique) Dataset for given DataIdentifiers
     * @throws NullPointerException                if dataIdentifiers is null
     * @throws DuplicateUniqueIdentifierException  if more than one DataInstance was registered for the given DataIdentifier
     * @throws UnregisteredDataIdentifierException if no DataInstance was registered for the given DataIdentifier
     */
    protected Dataset<Row> readInputs(Collection<DataIdentifier> dataIdentifiers) {
        Dataset<Row> dataset = null;

        for (DataIdentifier dataIdentifier : dataIdentifiers) {
            Dataset<Row> currentDataset = readInput(dataIdentifier);

            if (dataset == null) {
                dataset = currentDataset;
            } else {
                dataset = dataset.union(currentDataset);
            }
        }

        return dataset;
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
     * Reads Datasets registered for the given data identifier.
     * <p>
     * This method throws an exception if more than one DataInstance was
     * registered for the given DataIdentifier or if no DataIdentifier was
     * registered for the given DataIdentifier at all.
     * <p>
     * Any exception happening during reading of the Dataset is silently ignored
     * and Optional.empty() is returned instead.
     *
     * @param dataIdentifiers data identifiers used to resolve the data instance
     * @return the (unique) Dataset for given DataIdentifiers
     * @throws NullPointerException                if dataIdentifiers is null
     * @throws DuplicateUniqueIdentifierException  if more than one DataInstance was registered for the given DataIdentifier
     * @throws UnregisteredDataIdentifierException if no DataInstance was registered for the given DataIdentifier
     */
    protected Optional<Dataset<Row>> readOptionalInputs(Collection<DataIdentifier> dataIdentifiers) {
        Dataset<Row> dataset = null;

        for (DataIdentifier dataIdentifier : dataIdentifiers) {
            Optional<Dataset<Row>> oCurrentDataset = readOptionalInput(dataIdentifier);
            if (!oCurrentDataset.isPresent()) {
                continue;
            }

            if (dataset == null) {
                dataset = oCurrentDataset.get();
            } else {
                dataset = dataset.union(oCurrentDataset.get());
            }
        }

        if (dataset == null) {
            return Optional.empty();
        }

        return Optional.of(dataset);
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
        String path = dataInstance.resolvePath();
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

    private void writeOutput(DataIdentifier outputIdentifier, Dataset<Row> outputDataset) {
        Objects.requireNonNull(outputDataset, "outputDataset must not be null!");
        DataResolver dataResolver = dataResolverRegistry.getResolverFor(outputLocation);

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
        String path = dataInstance.resolvePath();
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
            Path targetFile = new Path(fileName);
            Path batchResultDir = new Path(targetPath);
            Configuration hadoopConfig = sparkContext.hadoopConfiguration();
            try (FileSystem fs = batchResultDir.getFileSystem(hadoopConfig)) {
                Path targetFilePath = fs.globStatus(new Path(batchResultDir, "part-*"))[0].getPath(); // TODO: multi-partition handling
                FileUtil.copy(fs, targetFilePath, fs, targetFile, true, true, hadoopConfig);
                // fs.rename(targetFilePath, targetFile);
                fs.delete(batchResultDir, true);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to rename result file!", ex);
            }
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
            try (FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration())) {
                Path targetFilePath = fs.globStatus(new Path(targetPath + "/part-*"))[0].getPath();
                fs.rename(targetFilePath, new Path(fileName));
                fs.delete(new Path(targetPath), true);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to rename result file!", ex);
            }
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
            try (FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration())) {
                Path targetFilePath = fs.globStatus(new Path(targetPath + "/part-*"))[0].getPath();
                fs.rename(targetFilePath, new Path(fileName));
                fs.delete(new Path(targetPath), true);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to rename result file!", ex);
            }
        }
    }

    protected enum WriteMode {
        DATE,
        CURRENT,
        BOTH
    }
}
