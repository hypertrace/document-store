import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStopContainer
import com.bmuschko.gradle.docker.tasks.image.DockerPullImage
import com.bmuschko.gradle.docker.tasks.network.DockerCreateNetwork
import com.bmuschko.gradle.docker.tasks.network.DockerRemoveNetwork

plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin") version "0.1.0"
  id("org.hypertrace.integration-test-plugin") version "0.1.0"
  id("com.bmuschko.docker-remote-api") version "6.4.0"
}

dependencies {
  api("com.typesafe:config:1.3.2")
  implementation("org.postgresql:postgresql:42.1.4")
  implementation("org.mongodb:mongodb-driver-sync:4.1.1")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.11.0")
  implementation("org.slf4j:slf4j-api:1.7.25")
  implementation("com.google.guava:guava-annotations:r03")
  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:2.19.0")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

tasks.test {
  useJUnitPlatform()
}

tasks.register<DockerCreateNetwork>("createIntegrationTestNetwork") {
  networkName.set("document-store-int-test");
}

tasks.register<DockerRemoveNetwork>("removeIntegrationTestNetwork") {
  networkId.set("document-store-int-test")
}

tasks.register<DockerPullImage>("pullMongoImage") {
  image.set("mongo:4.4.0")
}

tasks.register<DockerCreateContainer>("createMongoContainer") {
  dependsOn("createIntegrationTestNetwork")
  dependsOn("pullMongoImage")
  targetImageId(tasks.getByName<DockerPullImage>("pullMongoImage").image)
  containerName.set("mongo-local")
  hostConfig.network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
  hostConfig.portBindings.set(listOf("27017:27017"))
  hostConfig.autoRemove.set(true)
}

tasks.register<DockerStartContainer>("startMongoContainer") {
  dependsOn("createMongoContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
}

tasks.register<DockerStopContainer>("stopMongoContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
  finalizedBy("removeIntegrationTestNetwork")
}

tasks.register<DockerPullImage>("pullPostgresImage") {
  image.set("postgres:13.1")
}

tasks.register<DockerCreateContainer>("createPostgresContainer") {
  dependsOn("createIntegrationTestNetwork")
  dependsOn("pullPostgresImage")
  targetImageId(tasks.getByName<DockerPullImage>("pullPostgresImage").image)
  containerName.set("postgres-local")
  hostConfig.network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
  hostConfig.portBindings.set(listOf("5432:5432"))
  hostConfig.autoRemove.set(true)
}

tasks.register<DockerStartContainer>("startPostgresContainer") {
  dependsOn("createPostgresContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createPostgresContainer").containerId)
}

tasks.register<DockerStopContainer>("stopPostgresContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createPostgresContainer").containerId)
  finalizedBy("removeIntegrationTestNetwork")
}

tasks.integrationTest {
  useJUnitPlatform()
  dependsOn("startMongoContainer")
  dependsOn("startPostgresContainer")
  finalizedBy("stopMongoContainer")
  finalizedBy("stopPostgresContainer")
}

tasks.jacocoIntegrationTestReport {
  sourceSets(project(":document-store").sourceSets.getByName("main"))
}
