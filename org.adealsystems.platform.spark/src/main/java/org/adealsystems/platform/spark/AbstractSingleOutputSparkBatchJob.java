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
import org.adealsystems.platform.process.jdbc.JdbcConnectionProperties;
import org.adealsystems.platform.process.jdbc.JdbcConnectionRegistry;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;


public abstract class AbstractSingleOutputSparkBatchJob implements SparkDataProcessingJob {
    private static final String SEMICOLON = ";";
    private static final String COMMA = ",";
    private static final String PIPE = "|";
    private static final String SUCCESS_INDICATOR = "_SUCCESS";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSingleOutputSparkBatchJob.class);

    private final DataResolverRegistry dataResolverRegistry;
    private final DataLocation outputLocation;
    private final Set<DataIdentifier> outputIdentifiers;
    private final DataInstanceRegistry dataInstanceRegistry = new DataInstanceRegistry();
    private final JdbcConnectionRegistry jdbcConnectionRegistry = new JdbcConnectionRegistry();
    private final Map<String, Object> writerOptions = new HashMap<>();
    private final Map<DataIdentifier, String> processingStatus = new HashMap<>();
    private final boolean storeAsSingleFile;
    private final LocalDate invocationDate;

    private WriteMode writeMode;

    private boolean initSuccessful = true;
    private String initErrorMessage;

    private SparkSession sparkSession;
    private JavaSparkContext sparkContext;
    private long executionStartTimestamp = -1;
    private long executionDuration = -1;

    private SparkResultWriterInterceptor resultWriterInterceptor;

    private JobFinalizer jobFinalizer;

    private final Logger logger;
    private final DatasetLogger datasetLogger;

    public AbstractSingleOutputSparkBatchJob(
        DataResolverRegistry dataResolverRegistry,
        DataLocation outputLocation,
        DataIdentifier outputIdentifier,
        LocalDate invocationDate,
        boolean storeAsSingleFile
    ) {
        this.invocationDate = Objects.requireNonNull(invocationDate, "invocationDate must not be null!");
        this.dataResolverRegistry = Objects.requireNonNull(
            dataResolverRegistry,
            "dataResolverRegistry must not be null!"
        );
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

    public AbstractSingleOutputSparkBatchJob(
        DataResolverRegistry dataResolverRegistry,
        DataLocation outputLocation,
        DataIdentifier outputIdentifier,
        LocalDate invocationDate
    ) {
        this(dataResolverRegistry, outputLocation, outputIdentifier, invocationDate, false);
    }

    @Override
    public long getStartTimestamp() {
        if (executionStartTimestamp < 0) {
            throw new IllegalStateException("Start timestamp is only available after execute!");
        }

        return executionStartTimestamp;
    }

    @Override
    public long getDuration() {
        if (executionDuration < 0) {
            throw new IllegalStateException("Duration is only available after closing spark session!");
        }

        return executionDuration;
    }

    protected final void setWriteMode(WriteMode writeMode) {
        this.writeMode = Objects.requireNonNull(writeMode, "writeMode must not be null!");
    }

    @Override
    public WriteMode getWriteMode() {
        return writeMode;
    }

    @Override
    public void setInitFailure(String message) {
        initSuccessful = false;
        initErrorMessage = message;
    }

    @Override
    public boolean isInitSuccessful() {
        return initSuccessful;
    }

    @Override
    public String getInitErrorMessage() {
        return initErrorMessage;
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

    public void setJobFinalizer(JobFinalizer jobFinalizer) {
        this.jobFinalizer = jobFinalizer;
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

    protected DatasetLogger getDatasetLogger() {
        return datasetLogger;
    }

    public void addSessionAnalyser(String sessionCode, Consumer<DatasetLogger.AnalyserSession> analyser) {
        datasetLogger.addSessionAnalyser(sessionCode, analyser);
    }

    @Override
    public LocalDate getInvocationDate() {
        return invocationDate;
    }

    /**
     * Register your UDFs with the given UDFRegistration.
     *
     * @param udfRegistration the UDFRegistration to be used for registration.
     */
    protected void registerUdfs(UDFRegistration udfRegistration) {
        LOGGER.debug("No user defined functions registered");
    }

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
        executionStartTimestamp = System.currentTimeMillis();

        try {
            if (!initSuccessful) {
                throw new JobInitializationFailureException(initErrorMessage);
            }

            writeOutput(processData());
        }
        catch (Throwable th) {
            for (DataIdentifier dataId : getOutputIdentifiers()) {
                registerProcessingStatus(dataId, STATE_PROCESSING_ERROR);
            }

            throw th;
        }
    }

    @Override
    public void finalizeJob() {
        if (jobFinalizer == null) {
            logger.info("No job finalizer defined. Returning...");
            return;
        }

        if (sparkSession != null) {
            throw new IllegalStateException("Job can only be finalized after spark session has been closed!");
        }

        jobFinalizer.finalizeJob(this);
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

            executionDuration = System.currentTimeMillis() - executionStartTimestamp;
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
    protected void registerInputs(
        DataLocation dataLocation,
        Collection<DataIdentifier> inputIdentifiers,
        LocalDate date
    ) {
        Objects.requireNonNull(dataLocation, "dataLocation must not be null!");
        Objects.requireNonNull(inputIdentifiers, "inputIdentifiers must not be null!");
        Objects.requireNonNull(date, "date must not be null!");

        DataResolver inputDataResolver = dataResolverRegistry.getResolverFor(dataLocation);
        for (DataIdentifier inputIdentifier : inputIdentifiers) {
            dataInstanceRegistry.register(inputDataResolver.createDateInstance(inputIdentifier, date));
        }
    }

    protected void registerJdbcInput(JdbcConnectionProperties props, DataIdentifier inputIdentifier) {
        Objects.requireNonNull(props, "jdbcConnectionProperties must not be null!");
        Objects.requireNonNull(inputIdentifier, "inputIdentifier must not be null!");

        switch (inputIdentifier.getDataFormat()) {
            case ATHENA:
                LOGGER.debug("Registering athena input {} with properties {}", inputIdentifier, props);
                assertProperties(
                    props.getConnectionProperties(),
                    JdbcConnectionProperties.PROP_AWS_CREDENTIALS_PROVIDER,
                    JdbcConnectionProperties.PROP_URL,
                    JdbcConnectionProperties.PROP_DRIVER,
                    JdbcConnectionProperties.PROP_QUERY
                );
                break;

            case JDBC:
                LOGGER.debug("Registering jdbc input {} with properties {}", inputIdentifier, props);
                assertProperties(
                    props.getConnectionProperties(),
                    JdbcConnectionProperties.PROP_URL,
                    JdbcConnectionProperties.PROP_DRIVER,
                    JdbcConnectionProperties.PROP_QUERY
                );
                break;

            default:
                break;
        }

        jdbcConnectionRegistry.register(inputIdentifier, props);
    }

    private void assertProperties(Properties props, String... keys) {
        Objects.requireNonNull(props, "jdbcConnectionProperties must not be null!");
        Objects.requireNonNull(keys, "keys must not be null!");

        Set<String> missing = new HashSet<>();
        for (String key : keys) {
            if (!props.containsKey(key)) {
                missing.add(key);
            }
        }

        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("Missing jdbc connection properties " + missing + "!");
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
        switch (dataIdentifier.getDataFormat()) {
            case JDBC:
            case ATHENA:
                Optional<JdbcConnectionProperties> props = jdbcConnectionRegistry.resolve(dataIdentifier);
                if (props.isPresent()) {
                    return readJdbcInput(dataIdentifier, props.get());
                }
                break;

            default:
                DataInstance dataInstance = dataInstanceRegistry.resolveUnique(dataIdentifier).orElse(null);
                if (dataInstance != null) {
                    return readInput(dataInstance);
                }
                break;
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
        return readInputs(dataIdentifiers, null);
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
     * @param cleanser        function used to prepare the dataset
     * @return the (unique) Dataset for given DataIdentifiers
     * @throws NullPointerException                if dataIdentifiers is null
     * @throws DuplicateUniqueIdentifierException  if more than one DataInstance was registered for the given DataIdentifier
     * @throws UnregisteredDataIdentifierException if no DataInstance was registered for the given DataIdentifier
     */
    protected Dataset<Row> readInputs(
        Collection<DataIdentifier> dataIdentifiers,
        Function<Dataset<Row>, Dataset<Row>> cleanser
    ) {
        Dataset<Row> dataset = null;

        for (DataIdentifier dataIdentifier : dataIdentifiers) {
            Dataset<Row> currentDataset = readInput(dataIdentifier);

            if (cleanser != null) {
                currentDataset = cleanser.apply(currentDataset);
            }

            if (dataset == null) {
                dataset = currentDataset;
            }
            else {
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
            }
            catch (Throwable t) {
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
            }
            else {
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
            }
            catch (Throwable t) {
                logger.debug("Reading {} failed.", current, t);
            }
        }

        return result;
    }

    @SuppressWarnings("PMD.CloseResource")
    private Dataset<Row> readInput(DataInstance dataInstance) {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        String path = dataInstance.getPath();
        DataFormat dataFormat = dataInstance.getDataFormat();
        logger.info("Reading {} from '{}'.", dataInstance, path);

        SparkSession session = getSparkSession();
        switch (dataFormat) {
            case CSV_COMMA:
                return readCsvAsDataset(session, COMMA, path);

            case CSV_SEMICOLON:
                return readCsvAsDataset(session, SEMICOLON, path);

            case CSV_PIPE:
                return readCsvAsDataset(session, PIPE, path);

            case JSON:
                return readJsonAsDataset(session, path);

            case AVRO:
                return readAvroAsDataset(session, path);

            case PARQUET:
                return readParquetAsDataset(session, path);

            default:
                throw new UnsupportedDataFormatException(dataFormat);
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private Dataset<Row> readJdbcInput(DataIdentifier dataId, JdbcConnectionProperties props) {
        Objects.requireNonNull(dataId, "dataId must not be null!");

        Properties properties = props.getConnectionProperties();
        SparkSession session = getSparkSession();
        DataFormat dataFormat = dataId.getDataFormat();
        switch (dataFormat) {
            case JDBC:
                return readJdbc(session, properties);

            case ATHENA:
                return readAthenaJdbc(session, properties);

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
            throw new IllegalArgumentException(
                "More than one output identifier configured! Please write your output with specifying a corresponding identifier!");
        }

        // use the only one configured output identifier
        DataIdentifier outputIdentifier = outputIdentifiers.iterator().next();

        DataResolver dataResolver = getOutputDataResolver();

        datasetLogger.showInfo("About to write the following data for " + outputIdentifier, outputDataset);

        outputDataset = outputDataset.repartition(1); // always repartition(1) before writing!

        if (writeMode == WriteMode.BOTH) {
            outputDataset = outputDataset.cache(); // optimize for multiple outputs
        }

        if (writeMode == WriteMode.DATE || writeMode == WriteMode.BOTH) {
            DataInstance dateInstance = dataResolver.createDateInstance(outputIdentifier, invocationDate);

            if (resultWriterInterceptor == null) {
                logger.info("NOT registering result of {} because resultWriterInterceptor is null!", dateInstance);
            }
            else {
                logger.info("Registering result of {} with resultWriterInterceptor.", dateInstance);
                dateInstance = resultWriterInterceptor.registerResult(outputLocation, dateInstance, outputDataset);
            }

            writeOutputInternal(dateInstance, outputDataset, storeAsSingleFile, sparkContext, writerOptions);
        }

        if (writeMode == WriteMode.CURRENT || writeMode == WriteMode.BOTH) {
            DataInstance currentInstance = dataResolver.createCurrentInstance(outputIdentifier);
            writeOutputInternal(currentInstance, outputDataset, storeAsSingleFile, sparkContext, writerOptions);
        }
    }

    static void writeOutputInternal(
        DataInstance dataInstance,
        Dataset<Row> result,
        boolean storeAsSingleFile,
        JavaSparkContext sparkContext,
        Map<String, Object> writerOptions
    ) {
        Objects.requireNonNull(dataInstance, "dataInstance must not be null!");
        String path = dataInstance.getPath();
        LOGGER.info("Writing {} to '{}'.", dataInstance, path);
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

            case PARQUET:
                writeDatasetAsParquet(result, path, storeAsSingleFile, sparkContext, writerOptions);
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

    static Dataset<Row> readCsvAsDataset(SparkSession sparkSession, String delimiter, String fileName) {
        // TODO: double-check option names
        // https://spark.apache.org/docs/3.0.1/api/java/org/apache/spark/sql/DataFrameWriter.html#csv-java.lang.String-
        return sparkSession.read()
            .option("header", "true")
            //            .option("inferSchema", "true")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("sep", delimiter)
            .option("emptyValue", "")
            .csv(fileName);
    }

    static Dataset<Row> readJsonAsDataset(SparkSession sparkSession, String fileName) {
        return sparkSession.read()
            .json(fileName);
    }

    static Dataset<Row> readAvroAsDataset(SparkSession sparkSession, String fileName) {
        return sparkSession.read()
            .format("avro")
            .load(fileName);
    }

    static Dataset<Row> readParquetAsDataset(SparkSession sparkSession, String fileName) {
        return sparkSession.read()
            .format("parquet")
            .load(fileName);
    }

    static Dataset<Row> readJdbc(SparkSession sparkSession, Properties connectionProperties) {
        String jdbcUrl = connectionProperties.getProperty(JdbcConnectionProperties.PROP_URL);
        String query = connectionProperties.getProperty(JdbcConnectionProperties.PROP_QUERY);

        Properties props = cloneProperties(connectionProperties);

        props.remove(JdbcConnectionProperties.PROP_URL);
        props.remove(JdbcConnectionProperties.PROP_QUERY);

        return sparkSession.read()
            .jdbc(jdbcUrl, query, props);
    }

    static Dataset<Row> readAthenaJdbc(SparkSession sparkSession, Properties connectionProperties) {
        String jdbcUrl = connectionProperties.getProperty(JdbcConnectionProperties.PROP_URL);
        String query = connectionProperties.getProperty(JdbcConnectionProperties.PROP_QUERY);

        Properties props = cloneProperties(connectionProperties);

        props.remove(JdbcConnectionProperties.PROP_URL);
        props.remove(JdbcConnectionProperties.PROP_QUERY);

        return sparkSession.read()
            .jdbc(jdbcUrl, query, props);
    }

    private static Properties cloneProperties(Properties props) {
        if (props == null) {
            return null; // NOPMD
        }

        Properties result = new Properties();
        Enumeration<?> names = props.propertyNames();
        while (names.hasMoreElements()) {
            String name = String.valueOf(names.nextElement());
            String value = props.getProperty(name);
            result.setProperty(name, value);
        }

        return result;
    }

    static void writeDatasetAsCsv(
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

        // https://spark.apache.org/docs/3.0.1/api/java/org/apache/spark/sql/DataFrameWriter.html#csv-java.lang.String-
        DataFrameWriter<Row> writer = dataset.write()
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("sep", delimiter)
            .option("emptyValue", "");

        setWriterOptions(writer, writerOptions);

        writer.csv(targetPath);

        if (storeAsSingleFile) {
            moveFile(sparkContext, targetPath, fileName);
        }
    }

    static void writeDatasetAsJson(
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

        DataFrameWriter<Row> writer = dataset.write()
            .mode(SaveMode.Overwrite);

        setWriterOptions(writer, writerOptions);

        writer.json(targetPath);

        if (storeAsSingleFile) {
            moveFile(sparkContext, targetPath, fileName);
        }
    }

    static void writeDatasetAsAvro(
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

        DataFrameWriter<Row> writer = dataset.write()
            .mode(SaveMode.Overwrite)
            .format("avro");

        setWriterOptions(writer, writerOptions);

        writer.save(targetPath);

        if (storeAsSingleFile) {
            moveFile(sparkContext, targetPath, fileName);
        }
    }

    static void writeDatasetAsParquet(
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

        DataFrameWriter<Row> writer = dataset.write()
            .mode(SaveMode.Overwrite)
            .format("parquet");

        setWriterOptions(writer, writerOptions);

        writer.save(targetPath);

        if (storeAsSingleFile) {
            moveFile(sparkContext, targetPath, fileName);
        }
    }

    private static void setWriterOptions(DataFrameWriter<Row> writer, Map<String, Object> writerOptions) {
        for (Map.Entry<String, Object> option : writerOptions.entrySet()) {
            String key = option.getKey();
            Object value = option.getValue();
            if (value == null) {
                writer.option(key, null);
                continue;
            }
            Class<?> valueClass = value.getClass();
            if (Long.class.isAssignableFrom(valueClass)) {
                writer.option(key, (long) value);
            }
            else if (Double.class.isAssignableFrom(valueClass)) {
                writer.option(key, (double) value);
            }
            else if (Boolean.class.isAssignableFrom(valueClass)) {
                writer.option(key, (boolean) value);
            }
            else if (String.class.isAssignableFrom(valueClass)) {
                writer.option(key, (String) value);
            }
            else {
                String valueClassName = valueClass.getName();
                LOGGER.error("Ignoring unsupported value class {} for writer-option '{}'!", valueClassName, key);
                //writer.option(key, value.toString()); // null was already handled
            }
        }
    }

    public static FileSystem getFileSystem(JavaSparkContext sparkContext, String path) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConfig = sparkContext.hadoopConfiguration();
        hadoopConfig.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfig.setBoolean("fs.hdfs.impl.disable.cache", true);
        hadoopConfig.setBoolean("fs.s3a.impl.disable.cache", true);
        hadoopConfig.setBoolean("fs.s3n.impl.disable.cache", true);
        hadoopConfig.setBoolean("fs.s3.impl.disable.cache", true);
        Path sourcePath = new Path(path);
        return sourcePath.getFileSystem(hadoopConfig);
    }

    // this is broken code... but now it's in a single place
    static void moveFile(JavaSparkContext sparkContext, String source, String target) {
        try (FileSystem fs = getFileSystem(sparkContext, source)) {
            String successFilename = source + '/' + SUCCESS_INDICATOR;
            LOGGER.debug("successFilename: {}", successFilename);
            Path successPath = new Path(successFilename);
            LOGGER.debug("successPath: {}", successPath);

            int loopCount = 0;
            while (!fs.exists(successPath)) {
                try {
                    if (loopCount >= 600) { // 10 minutes
                        throw new IllegalStateException("No result file found: '" + successFilename + "'");
                    }
                    loopCount++;

                    LOGGER.info("Waiting for appearance of {}...", successFilename);
                    Thread.sleep(1_000);
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException("Exception while waiting for success file!", e);
                }
            }

            LOGGER.debug("Found successPath");

            Path pathPattern = new Path(source + "/part-*");
            FileStatus[] globs = fs.globStatus(pathPattern);
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

            LOGGER.debug("Globs for path {}: {}", pathPattern, globs);
            if (globs.length != 1) {
                LOGGER.error("Expected one part file for {} but found {}! {}", source, globs.length, globs);
                throw new IllegalStateException("Expected one part file for " + source + " but found " + globs.length);
            }

            Path sourcePath = globs[0].getPath();
            Path targetPath = new Path(target);

            if (fs.exists(targetPath)) {
                LOGGER.debug("Deleting existing path '{}'", targetPath);
                if (!fs.delete(targetPath, true)) {
                    LOGGER.error(
                        "Unable to delete already existing target '{}'! Trying to overwrite it ...",
                        targetPath
                    );
                }
            }

            LOGGER.debug("About to rename sourcePath: {} to targetPath: {}", sourcePath, targetPath);
            if (fs.rename(sourcePath, targetPath)) {
                if (!fs.delete(new Path(source), true)) {
                    LOGGER.warn("Failed to delete source: {}", source);
                }
                return;
            }

            LOGGER.warn("Failed to rename sourcePath: {} to targetPath: {}", sourcePath, targetPath);
            LOGGER.debug("About to copy sourcePath: {} to targetPath: {}", sourcePath, targetPath);
            if (!FileUtil.copy(fs, sourcePath, fs, targetPath, true, sparkContext.hadoopConfiguration())) {
                LOGGER.warn("Failed to copy sourcePath: {} to targetPath: {}", sourcePath, targetPath);
            }
            else {
                if (!fs.delete(new Path(source), true)) {
                    LOGGER.warn("Failed to delete source: {}", source);
                }
            }
        }
        catch (IOException ex) {
            throw new IllegalStateException("Unable to rename result file!", ex);
        }
    }
}
