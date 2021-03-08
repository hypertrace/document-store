plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin") version "0.1.0"
  id("org.hypertrace.integration-test-plugin") version "0.1.0"
}

dependencies {
  api("com.typesafe:config:1.3.2")
  implementation("org.postgresql:postgresql:42.2.13")
  implementation("org.mongodb:mongodb-driver-sync:4.1.2")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.11.0")
  implementation("org.slf4j:slf4j-api:1.7.25")
  implementation("com.google.guava:guava-annotations:r03")
  implementation("org.apache.commons:commons-lang3:3.10")
  implementation("net.jodah:failsafe:2.4.0")
  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:2.19.0")

  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  integrationTestImplementation("com.github.java-json-tools:json-patch:1.13")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-api:5.6.2")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-params:5.6.2")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-engine:5.6.2")
  integrationTestImplementation("org.testcontainers:testcontainers:1.15.2")
  integrationTestImplementation("org.testcontainers:junit-jupiter:1.15.2")

}

tasks.test {
  useJUnitPlatform()
}

tasks.integrationTest {
  useJUnitPlatform()
}

tasks.jacocoIntegrationTestReport {
  sourceSets(project(":document-store").sourceSets.getByName("main"))
}
