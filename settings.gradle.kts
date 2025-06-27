rootProject.name = "document-store"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://us-maven.pkg.dev/hypertrace-repos/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.3.0"
  id("org.hypertrace.jacoco-report-plugin") version "0.3.0" apply false
  id("org.hypertrace.integration-test-plugin") version "0.3.0" apply false
}

include(":document-store")
