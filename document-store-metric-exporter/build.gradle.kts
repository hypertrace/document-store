plugins {
  `java-library`
  jacoco
  id("org.hypertrace.publish-plugin")
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":document-store"))

  annotationProcessor(libs.org.projectlombok.lombok)
  compileOnly(libs.org.projectlombok.lombok)

  implementation(libs.hypertrace.service.framework.metrics)
  implementation(libs.javax.inject)
  implementation(libs.google.guice)
}
