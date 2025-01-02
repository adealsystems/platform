build:
    ./gradlew clean build --refresh-dependencies -Dprerelease=alpha.0
publish-local:
    ./gradlew clean publishToMavenLocal --warn --stacktrace

publish-dry:
    ./gradlew clean publish --warn --stacktrace --dry-run

publish:
    ./gradlew clean publish --warn --stacktrace
