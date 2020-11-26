/*
 * Copyright 2020 ADEAL Systems GmbH
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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public abstract class AbstractSparkBatchJob implements SparkDataProcessingJob {
    private static final String SEMICOLON = ";";
    private static final String COMMA = ",";
    private final DataResolverRegistry dataResolverRegistry;
    private final DataLocation outputLocation;
    private final DataIdentifier outputIdentifier;
    private final DataInstanceRegistry dataInstanceRegistry = new DataInstanceRegistry();
    private final LocalDate invocationDate;
    private final DatasetLogger datasetLogger;

    private WriteMode writeMode;

    private SparkSession sparkSession;
    private JavaSparkContext sparkContext;

    private Broadcast<LocalDateTime> broadInvocationIdentifier;
    private final Logger logger;

    public AbstractSparkBatchJob(DataResolverRegistry dataResolverRegistry, DataLocation outputLocation, DataIdentifier outputIdentifier, LocalDate invocationDate) {
        this.invocationDate = Objects.requireNonNull(invocationDate, "invocationDate must not be null!");
        this.dataResolverRegistry = Objects.requireNonNull(dataResolverRegistry, "dataResolverRegistry must not be null!");
        this.outputLocation = Objects.requireNonNull(outputLocation, "outputLocation must not be null!");
        this.outputIdentifier = Objects.requireNonNull(outputIdentifier, "outputIdentifier must not be null!");
        this.logger = LoggerFactory.getLogger(getClass());
        setWriteMode(WriteMode.DATE);
        DatasetLogger.Context dlc = DatasetLogger.newContext();
        dlc.setLogger(logger);
        this.datasetLogger = new DatasetLogger(dlc);
    }

    protected final void setWriteMode(WriteMode writeMode) {
        this.writeMode = Objects.requireNonNull(writeMode, "writeMode must not be null!");
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
     * @see #getOutputIdentifier() method to access the output DataIdentifier.
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
        broadInvocationIdentifier = getSparkContext().broadcast(LocalDateTime.now(Clock.systemDefaultZone()));
        writeOutput(processData());
    }


    @Override
    public DataIdentifier getOutputIdentifier() {
        return outputIdentifier;
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
        String path = dataInstance.resolvePath();
        DataFormat dataFormat = dataInstance.getDataFormat();
        logger.info("Reading {} from '{}'.", dataInstance, path);
        switch (dataFormat) {
            case CSV_COMMA:
                return readCsvAsDataset(COMMA, path);
            case CSV_SEMICOLON:
                return readCsvAsDataset(SEMICOLON, path);
            case JSON:
                return readJsonAsDataset(path);
            case AVRO:
                return readAvroAsDataset(path);
            default:
                throw new UnsupportedDataFormatException(dataFormat);
        }
    }

    private void writeOutput(Dataset<Row> outputDataset) {
        Objects.requireNonNull(outputDataset, "outputDataset must not be null!");
        DataResolver dataResolver = dataResolverRegistry.getResolverFor(outputLocation);

        getDatasetLogger().showInfo("About to write the following data for " + outputIdentifier, outputDataset);
        outputDataset = outputDataset.repartition(1); // always repartition(1) before writing!
        if (writeMode == WriteMode.DATE || writeMode == WriteMode.BOTH) {
            DataInstance dateInstance = dataResolver.createDateInstance(outputIdentifier, invocationDate);
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
                writeDatasetAsCsv(COMMA, result, path);
                return;
            case CSV_SEMICOLON:
                writeDatasetAsCsv(SEMICOLON, result, path);
                return;
            case JSON:
                writeDatasetAsJson(result, path);
                return;
            case AVRO:
                writeDatasetAsAvro(result, path);
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

    private static void writeDatasetAsCsv(String delimiter, Dataset<Row> dataset, String fileName) {
        // TODO: option names
        // https://spark.apache.org/docs/2.4.3/api/java/org/apache/spark/sql/DataFrameWriter.html#csv-java.lang.String-
        dataset.write() //
                .mode(SaveMode.Overwrite) //
                .option("header", "true") //
                .option("delimiter", delimiter) // TODO: option names
                .option("emptyValue", "") // TODO: option names
                .csv(fileName);
    }

    private static void writeDatasetAsJson(Dataset<Row> dataset, String fileName) {
        dataset.write() //
                .mode(SaveMode.Overwrite) //
                .json(fileName);
    }

    private static void writeDatasetAsAvro(Dataset<Row> dataset, String fileName) {
        dataset.write() //
                .mode(SaveMode.Overwrite) //
                .format("avro") //
                .save(fileName);
    }

    protected enum WriteMode {
        DATE,
        CURRENT,
        BOTH
    }
}
