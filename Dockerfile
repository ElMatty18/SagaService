# syntax=docker/dockerfile:1.7
# Dockerfile único multi-módulo:  docker build --build-arg SERVICE=order-service .
# ---------------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
ARG SERVICE
WORKDIR /workspace

# 1) Sólo los POMs primero: la capa de dependencias se cachea mientras no cambien.
COPY pom.xml .
COPY order-service/pom.xml order-service/
COPY catalog-service/pom.xml catalog-service/
COPY api-gateway/pom.xml api-gateway/
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B -pl ${SERVICE} -am dependency:go-offline -DexcludeReactor=true || true

# 2) Código fuente y empaquetado (los tests corren con `make test`, no en la imagen).
COPY ${SERVICE}/src ${SERVICE}/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B -pl ${SERVICE} -am -Dmaven.test.skip=true package \
 && cp ${SERVICE}/target/${SERVICE}-*.jar /workspace/app.jar \
 && java -Djarmode=tools -jar /workspace/app.jar extract --layers --launcher --destination /workspace/extracted

# ---------------------------------------------------------------------------------
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 app
# Capas de Spring Boot: dependencias (cambian poco) separadas del código (cambia siempre).
COPY --from=build /workspace/extracted/dependencies/ ./
COPY --from=build /workspace/extracted/spring-boot-loader/ ./
COPY --from=build /workspace/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/extracted/application/ ./
USER app
# MaxRAMPercentage: la JVM respeta el límite de memoria del contenedor.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
