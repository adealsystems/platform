build prerelease:
    ./gradlew clean build --refresh-dependencies -Dprerelease={{prerelease}}
publish-local:
    ./gradlew clean publishToMavenLocal --warn --stacktrace

publish-dry:
    ./gradlew clean publish --warn --stacktrace --dry-run

publish:
    ./gradlew clean publish --no-configure-on-demand --warn --stacktrace
