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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.adealsystems.platform.spark.AbstractSingleOutputSparkBatchJob.readAthenaJdbc;
import static org.adealsystems.platform.spark.AbstractSingleOutputSparkBatchJob.readAvroAsDataset;
import static org.adealsystems.platform.spark.AbstractSingleOutputSparkBatchJob.readCsvAsDataset;
import static org.adealsystems.platform.spark.AbstractSingleOutputSparkBatchJob.readJdbc;
import static org.adealsystems.platform.spark.AbstractSingleOutputSparkBatchJob.readJsonAsDataset;
import static org.adealsystems.platform.spark.AbstractSingleOutputSparkBatchJob.readParquetAsDataset;
import static org.adealsystems.platform.spark.AbstractSingleOutputSparkBatchJob.writeOutputInternal;

public abstract class AbstractMultipleOutputSparkBatchJob implements SparkDataProcessingJob {
    private static final String SEMICOLON = ";";
    private static final String COMMA = ",";
    private static final String PIPE = "|";

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMultipleOutputSparkBatchJob.class);

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
        this.datasetLogger = new DatasetLogger(dlc, sparkContext);
    }

    public AbstractMultipleOutputSparkBatchJob(DataResolverRegistry dataResolverRegistry, DataLocation outputLocation, Collection<DataIdentifier> outputIdentifiers, LocalDate invocationDate) {
        this(dataResolverRegistry, outputLocation, outputIdentifiers, invocationDate, false);
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

    public void setResultWriterInterceptor(SparkResultWriterInterceptor sparkResultWriterInterceptor) {
        this.resultWriterInterceptor = sparkResultWriterInterceptor;
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

    public void addSessionAnalyser(String sessionCode, BiConsumer<DatasetLogger.AnalyserSession, String> analyser) {
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
    protected abstract Map<DataIdentifier, Dataset<Row>> processData();

    @Override
    public void execute() {
        executionStartTimestamp = System.currentTimeMillis();

        try {
            if (!initSuccessful) {
                throw new JobInitializationFailureException(initErrorMessage);
            }

            Map<DataIdentifier, Dataset<Row>> results = processData();
            for (Map.Entry<DataIdentifier, Dataset<Row>> entry : results.entrySet()) {
                DataIdentifier outputIdentifier = entry.getKey();
                Dataset<Row> dataset = entry.getValue();
                writeOutput(outputIdentifier, dataset);
            }
        } catch (Throwable th) {
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
    protected void registerInputs(DataLocation dataLocation, Collection<DataIdentifier> inputIdentifiers, LocalDate date) {
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
                assertProperties(props.getConnectionProperties(),
                    JdbcConnectionProperties.PROP_AWS_CREDENTIALS_PROVIDER,
                    JdbcConnectionProperties.PROP_URL,
                    JdbcConnectionProperties.PROP_DRIVER,
                    JdbcConnectionProperties.PROP_QUERY);
                break;
            case JDBC:
                assertProperties(props.getConnectionProperties(),
                    JdbcConnectionProperties.PROP_URL,
                    JdbcConnectionProperties.PROP_DRIVER,
                    JdbcConnectionProperties.PROP_QUERY);
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
    protected Dataset<Row> readInputs(Collection<DataIdentifier> dataIdentifiers, Function<Dataset<Row>, Dataset<Row>> cleanser) {
        Dataset<Row> dataset = null;

        for (DataIdentifier dataIdentifier : dataIdentifiers) {
            Dataset<Row> currentDataset = readInput(dataIdentifier);

            if (cleanser != null) {
                currentDataset = cleanser.apply(currentDataset);
            }

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
        String path = dataInstance.getPath();
        DataFormat dataFormat = dataInstance.getDataFormat();
        logger.info("Reading {} from '{}'.", dataInstance, path);
        switch (dataFormat) {
            case CSV_COMMA:
                return readCsvAsDataset(getSparkSession(), COMMA, path);
            case CSV_SEMICOLON:
                return readCsvAsDataset(getSparkSession(), SEMICOLON, path);
            case CSV_PIPE:
                return readCsvAsDataset(getSparkSession(), PIPE, path);
            case JSON:
                return readJsonAsDataset(getSparkSession(), path);
            case AVRO:
                return readAvroAsDataset(getSparkSession(), path);
            case PARQUET:
                return readParquetAsDataset(getSparkSession(), path);
            default:
                throw new UnsupportedDataFormatException(dataFormat);
        }
    }

    @Override
    public DataResolver getOutputDataResolver() {
        return dataResolverRegistry.getResolverFor(outputLocation);
    }

    private void writeOutput(DataIdentifier outputIdentifier, Dataset<Row> outputDataset) {
        Objects.requireNonNull(outputDataset, "outputDataset must not be null!");
        DataResolver dataResolver = getOutputDataResolver();

        getDatasetLogger().showInfo("About to write the following data for " + outputIdentifier, outputDataset);

        outputDataset = outputDataset.repartition(1); // always repartition(1) before writing!

        if (writeMode == WriteMode.BOTH) {
            outputDataset = outputDataset.cache(); // optimize for multiple outputs
        }

        if (writeMode == WriteMode.DATE || writeMode == WriteMode.BOTH) {
            DataInstance dateInstance = dataResolver.createDateInstance(outputIdentifier, invocationDate);

            if (resultWriterInterceptor == null) {
                logger.info("NOT registering result of {} because resultWriterInterceptor is null!", dateInstance);
            } else {
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
}
