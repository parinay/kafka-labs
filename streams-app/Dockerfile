# Stage 1: Build using Gradle
FROM gradle:7.6.0-jdk17 AS builder
WORKDIR /home/app
COPY . .
RUN gradle shadowJar

# Stage 2: Run the application
FROM openjdk:17-jdk-slim
WORKDIR /app
COPY --from=builder /home/app/build/libs/app.jar app.jar
CMD ["java", "-jar", "app.jar"]