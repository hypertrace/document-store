rootProject.name = "document-store"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://hypertrace.jfrog.io/artifactory/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.2.1"
  id("org.hypertrace.jacoco-report-plugin") version "0.2.1" apply false
  id("org.hypertrace.integration-test-plugin") version "0.2.0" apply false
}

include(":document-store")
