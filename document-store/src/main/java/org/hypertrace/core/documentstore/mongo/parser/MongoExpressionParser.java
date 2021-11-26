package org.hypertrace.core.documentstore.mongo.parser;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.query.Query;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
class MongoExpressionParser {
  private static final String UNSUPPORTED_OPERATION = "No MongoDB support available for: '%s'";
  protected final Query query;

  protected static <T> UnsupportedOperationException getUnsupportedOperationException(T t) {
    return new UnsupportedOperationException(String.format(UNSUPPORTED_OPERATION, t));
  }
}
