# Runtime image for the mail service. The Quarkus fast-jar is built beforehand on the host
# (`mvn package -DskipTests` produces target/quarkus-app). SMTP and the API key come from env at
# runtime (QUARKUS_MAILER_HOST/_PORT, MAIL_API_KEY); see application.properties.
FROM eclipse-temurin:25-jre
WORKDIR /app
COPY target/quarkus-app/ ./
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "quarkus-run.jar"]
