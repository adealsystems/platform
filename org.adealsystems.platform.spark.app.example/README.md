# Example App

## Preparing execution
The `application.properties` in module's root contains paths to input & output 
folders of batch jobs.

`data.location.base.directory.input=./data/export`
`data.location.base.directory.output=./data/batch`

A sample input file for the `ExampleBatchJob` is available in `./data/export/...` folder.

Before start a job, verify if all needed files are available on specified paths.

## Execute without any arguments
This will fail if more than one job has been defined. It will print a list of all valid
job identifiers in that case.  
Otherwise, the single possible job will be executed. 

`../gradlew run`

## Executing a job for a specific invocation date
If no invocation date has been specified, the current date will be used automatically.

`../gradlew run --args='--job=example:output:CSV_SEMICOLON --invocation-date=2020-04-01'`

## Executing with remote profile
This fails if executed locally due to missing configuration.

`--remote` would be specified if executing on AWS.
 
`../gradlew run --args='--job=example:output:CSV_SEMICOLON --remote'`

## AWS-Deployment
- Upload the jar from `build/libs` to AWS. Make sure to take the one including `-all`.
- In the EMR step...
  - ... the spark-submit options *must* include `--class org.adealsystems.platform.spark.main.Application`.  
  This option is the same for all ADEAL Systems Platform Spark apps!
  - ... the "Application location" option must reference the location of the uploaded JAR.
  - ... the "Arguments" option must contain a job definition like `--job=example:output:CSV_SEMICOLON`.
