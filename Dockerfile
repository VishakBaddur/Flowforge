# Runtime image for any Flowforge service:  docker build --build-arg MODULE=api .
# Build the jars first:                      mvn -DskipTests package
FROM eclipse-temurin:21-jre AS extract
ARG MODULE
WORKDIR /build
COPY flowforge-${MODULE}/target/flowforge-${MODULE}-*-SNAPSHOT.jar app.jar
# Split the fat jar into layers: dependencies change rarely, application code changes often.
RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination extracted

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 flowforge
WORKDIR /app
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/spring-boot-loader/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./
USER flowforge
# Size the heap from the container's memory limit; crash fast on OOM so the orchestrator restarts us.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
