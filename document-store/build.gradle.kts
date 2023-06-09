plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin") version "0.2.0"
  id("org.hypertrace.integration-test-plugin") version "0.2.0"
}

dependencies {
  api("com.typesafe:config:1.4.2")
  annotationProcessor("org.projectlombok:lombok:1.18.26")
  compileOnly("org.projectlombok:lombok:1.18.26")
  implementation("org.apache.commons:commons-collections4:4.4")
  implementation("org.postgresql:postgresql:42.5.4")
  implementation("org.mongodb:mongodb-driver-sync:4.9.0")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.15.1")
  implementation("org.slf4j:slf4j-api:2.0.5")
  implementation("org.apache.commons:commons-lang3:3.12.0")
  implementation("net.jodah:failsafe:2.4.4")
  implementation("com.google.guava:guava:31.1-jre")
  implementation("org.apache.commons:commons-dbcp2:2.9.0")

  testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
  testImplementation("org.mockito:mockito-core:5.2.0")
  testImplementation("org.mockito:mockito-inline:5.2.0")
  testImplementation("org.mockito:mockito-junit-jupiter:5.2.0")

  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
  integrationTestImplementation("com.github.java-json-tools:json-patch:1.13")
  integrationTestImplementation("org.testcontainers:testcontainers:1.17.6")
  integrationTestImplementation("org.testcontainers:junit-jupiter:1.17.6")
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
