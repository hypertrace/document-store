plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin") version "0.2.0"
  id("org.hypertrace.integration-test-plugin") version "0.2.0"
}

dependencies {
  api("com.typesafe:config:1.3.2")
  implementation("junit:junit:4.13.1")
  annotationProcessor("org.projectlombok:lombok:1.18.22")
  compileOnly("org.projectlombok:lombok:1.18.22")
  compileOnly("javax.validation:validation-api:2.0.1.Final")
  compileOnly("org.hibernate:hibernate-validator-annotation-processor:6.0.11.Final")
  implementation("javax.el:javax.el-api:3.0.1-b06")
  implementation("org.hibernate:hibernate-validator:6.1.3.Final")
  implementation("org.apache.commons:commons-collections4:4.4")
  implementation("org.glassfish.web:javax.el:2.2.6")
  implementation("org.postgresql:postgresql:42.2.13")
  implementation("org.mongodb:mongodb-driver-sync:4.1.2")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.11.0")
  implementation("org.slf4j:slf4j-api:1.7.25")
  implementation("com.google.guava:guava-annotations:r03")
  implementation("org.apache.commons:commons-lang3:3.10")
  implementation("net.jodah:failsafe:2.4.0")
  testImplementation("org.junit.jupiter:junit-jupiter-api:5.7.0")
  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.junit.jupiter:junit-jupiter-params:5.7.0")
  testImplementation("org.mockito:mockito-core:2.19.0")
  testImplementation("org.mongodb:mongodb-driver-sync:4.1.2")
  testImplementation("org.mockito:mockito-junit-jupiter:4.0.0")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.7.0")

  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  integrationTestImplementation("com.github.java-json-tools:json-patch:1.13")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-api:5.8.1")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-params:5.7.0")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter-engine:5.7.0")
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
