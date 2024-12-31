build:
    ./gradlew clean build --refresh-dependencies
publish-local:
    ./gradlew clean publishToMavenLocal --warn --stacktrace

publish-dry:
    ./gradlew clean publish --warn --stacktrace --dry-run

publish:
    ./gradlew clean publish --warn --stacktrace
