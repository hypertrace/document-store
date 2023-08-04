plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.integration-test-plugin")
}

dependencies {
  api(libs.com.typesafe.config)
  annotationProcessor(libs.org.projectlombok.lombok)
  compileOnly(libs.org.projectlombok.lombok)
  implementation(libs.org.apache.commons.commons.collections4)
  implementation(libs.org.postgresql)
  implementation(libs.org.mongodb.mongodb.driver.sync)
  implementation(libs.com.fasterxml.jackson.core.jackson.databind)
  implementation(libs.org.slf4j.slf4j.api)
  implementation(libs.org.apache.commons.commons.lang3)
  implementation(libs.net.jodah.failsafe)
  implementation(libs.com.google.guava)
  implementation(libs.org.apache.commons.commons.dbcp2)
  implementation(libs.hypertrace.service.framework.metrics)
  implementation(libs.javax.inject)
  implementation(libs.google.guice)

  testImplementation(libs.org.junit.jupiter.junit.jupiter)
  testImplementation(libs.org.mockito.mockito.core)
  testImplementation(libs.org.mockito.mockito.inline)
  testImplementation(libs.org.mockito.mockito.junit.jupiter)

  integrationTestImplementation(libs.org.junit.jupiter.junit.jupiter)
  integrationTestImplementation(libs.com.github.java.json.tools.json.patch)
  integrationTestImplementation(libs.org.testcontainers)
  integrationTestImplementation(libs.org.testcontainers.junit.jupiter)
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
