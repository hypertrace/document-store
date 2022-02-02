package org.hypertrace.core.documentstore.mongo;

public final class MongoUtils {
  public static final String FIELD_SEPARATOR = ".";
  public static final String PREFIX = "$";
  private static final String UNSUPPORTED_OPERATION = "No MongoDB support available for: '%s'";

  public static <T> UnsupportedOperationException getUnsupportedOperationException(T t) {
    return new UnsupportedOperationException(String.format(UNSUPPORTED_OPERATION, t));
  }

  public static String encodeKey(final String key) {
    if (key == null) {
      return null;
    }

    return key.replace("\\", "\\\\").replace(PREFIX, "\\u0024").replace(FIELD_SEPARATOR, "\\u002e");
  }

  public static String decodeKey(final String key) {
    if (key == null) {
      return null;
    }

    return key.replace("\\u002e", FIELD_SEPARATOR).replace("\\u0024", PREFIX).replace("\\\\", "\\");
  }
}
