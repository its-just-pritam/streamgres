FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY . .
RUN mvn clean package -D skipTests

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /build/target/*.jar broker.jar
EXPOSE 8080, 9090
ENTRYPOINT ["java", "-jar", "broker.jar"]