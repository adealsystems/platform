# README

## Java

Download Java from [adoptopenjdk.net](https://adoptopenjdk.net/).

## Gradle
This repository uses [Gradle](https://gradle.org/) as the build-system.

### Build and IDE setup
Build by executing `./gradlew` (or `gradlew.bat` on Windows).  
This builds, performs unit-tests and checks if all [checkstyle](https://checkstyle.sourceforge.io/) and [PMD](https://pmd.github.io/) rules are honored.  
It also downloads the specified Gradle distribution automatically if necessary. There's no need to install Gradle on the OS level.

Generate IDEA project files by executing `./gradlew idea`.

Generate Eclipse project files by executing `./gradlew eclipse`.

Publish to local Maven repository (i.e. `~/.m2/repository`) by executing `./gradlew publishToMavenLocal`.  
This is NOT done by default because building Javadoc, Javadoc-JARs and Source-JARs (necessary for Maven) is time-consuming.

### Documentation
- [Gradle User Manual](https://docs.gradle.org/current/userguide/userguide.html)
- [Gradle Build Language Reference](https://docs.gradle.org/current/dsl/)

### Plugins

- [gradle-git-properties](https://github.com/n0mer/gradle-git-properties) - Gradle plugin for `git.properties` file generation
- [Gradle Shadow Plugin](https://imperceptiblethoughts.com/shadow/)
- [Spring Boot Gradle Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/current/gradle-plugin/reference/html/)
- [Spring Dependency Management Plugin](https://docs.spring.io/dependency-management-plugin/docs/current-SNAPSHOT/reference/html/)

## Quality

### License
The `licenseFormat` task applies/updates the license of all relevant files.

### Javadoc
Generate documentation by executing `./gradlew javadoc`.

The generated Javadoc can be accessed at `/build/docs/javadoc/index.html` of
the respective project.

Writing properly formatted Javadoc can be quite painful, especially
with Java version beyond Java 8.

The `javadoc` task will fail in case of issues.

And it will do so quite often...

### Testing

While writing tests with [JUnit 4.x](https://junit.org/junit4/) is still supported, it is also possible to write tests with [Spock](http://spockframework.org/spock/docs/1.3/index.html).

Try it. It's fun.

### PMD

[PMD](https://pmd.github.io/) doesn't just check for wrong code but also for questionable code. Because of this, it can be necessary to [suppress one of its warnings](https://pmd.github.io/pmd-6.22.0/pmd_userdocs_suppressing_warnings.html) after checking that it isn't actually a problem at all.

One good example for this is the [AvoidInstantiatingObjectsInLoops](https://pmd.github.io/pmd-6.22.0/pmd_rules_java_performance.html#avoidinstantiatingobjectsinloops) rule.

Suppression can be performed on the line level by appending a `// NOPMD - explanation why this is fine` comment to the line producing the warning. This disables *all* PMD warnings for the line, so be careful.

Suppression on method or field level can be performed by annotating with `@SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")` or `@SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops", "PMD.SomeOtherRule"})` in case of multiple warnings.

The configuration of PMD is located at `config/pmd`.

### Checkstyle

The configuration of Checkstyle is located at `config/checkstyle`.

### Policeman's Forbidden API Checker

[Policeman's Forbidden API Checker](https://github.com/policeman-tools/forbidden-apis) allows to parse Java byte code to find invocations of method/class/field signatures and fail build (Apache Ant, Apache Maven, or Gradle).

- [Wiki](https://github.com/policeman-tools/forbidden-apis/wiki)
- [Signatures](https://github.com/policeman-tools/forbidden-apis/wiki/BundledSignatures)

### AWS
- EMR logs are located at `(log-base-dir)/containers/application_(timestamp)/container_(timestamp)_01_000001/stderr`

## Dependencies
### slf4j
- Dependency: `org.slf4j:slf4j-api`
- [Website](http://slf4j.org/)
- [Manual](http://slf4j.org/manual.html)

### Logback
`slf4j` (API) and `logback` (backend implementation) are the unofficial successor of `log4j`, written by the same author.

- Dependency: `ch.qos.logback:logback-classic`
- [Website](http://logback.qos.ch/)
- [Documentation](http://logback.qos.ch/documentation.html)

### log4j2
This is the official successor of `log4j`. This is the less-conservative choice but may make sense for reasons outlined on the website.

- [Website](https://logging.apache.org/log4j/2.x/)

### Testcontainers
[Testcontainers](https://www.testcontainers.org/) is a Java library that supports JUnit tests, providing lightweight, throwaway instances of common databases, Selenium web browsers, or anything else that can run in a Docker container.

### Apache Spark
- [Website](https://spark.apache.org/)
- [Documentation](https://spark.apache.org/docs/latest/)
  - [Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)
  - [Amazon EMR - Adding a Spark Step](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html)

### Apache Commons

#### commons-csv
- Dependency: `org.apache.commons:commons-csv`
- [Website](https://commons.apache.org/proper/commons-csv/)
- [User Guide](https://commons.apache.org/proper/commons-csv/user-guide.html)

#### commons-io
- [User guide](https://commons.apache.org/proper/commons-io/description.html)

#### commons-lang3
- [Website](https://commons.apache.org/proper/commons-lang/)

#### commons-math3
- [Website](https://commons.apache.org/proper/commons-math/)

#### commons-text
- [Website](https://commons.apache.org/proper/commons-text/)
- [User guide](https://commons.apache.org/proper/commons-text/userguide.html)

### picocli
- Dependency: `info.picocli:picocli`
- [Website](https://picocli.info/)
