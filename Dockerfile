FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests -B

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/cdb-applier-1.0-SNAPSHOT.jar app.jar
EXPOSE ${APPLIER_PORT}
ENTRYPOINT ["java", "-jar", "app.jar"]