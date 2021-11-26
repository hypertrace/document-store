package org.hypertrace.core.documentstore.mongo.parser;

class MongoParserUtils {
  private static final String UNSUPPORTED_OPERATION = "No MongoDB support available for: '%s'";

  static <T> UnsupportedOperationException getUnsupportedOperationException(T t) {
    return new UnsupportedOperationException(String.format(UNSUPPORTED_OPERATION, t));
  }
}
