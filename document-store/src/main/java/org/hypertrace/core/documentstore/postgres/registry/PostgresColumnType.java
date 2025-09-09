package org.hypertrace.core.documentstore.postgres.registry;

/**
 * Enumeration of PostgreSQL column types supported by the document store. This enum maps PostgreSQL
 * data types to their corresponding Java representations and determines the appropriate query
 * generation strategy.
 */
public enum PostgresColumnType {
  /** PostgreSQL text/varchar types - mapped to Java String */
  TEXT,

  /** PostgreSQL bigint/int8 types - mapped to Java Long */
  BIGINT,

  /** PostgreSQL double precision/float8 types - mapped to Java Double */
  DOUBLE_PRECISION,

  /** PostgreSQL boolean/bool types - mapped to Java Boolean */
  BOOLEAN,

  /** PostgreSQL text array types - mapped to Java String[] */
  TEXT_ARRAY,

  /** PostgreSQL jsonb/json types - existing document storage format */
  JSONB;

  /**
   * Determines if this column type represents a first-class (non-JSON) field. First-class fields
   * are stored as native PostgreSQL types rather than within JSONB documents.
   *
   * @return true if this is a first-class field type, false if it's a JSON document field
   */
  public boolean isFirstClassField() {
    return this != JSONB;
  }

  /**
   * Maps PostgreSQL data type names to PostgresColumnType enum values.
   *
   * @param dataType the PostgreSQL data type name (e.g., "text", "bigint", "boolean")
   * @param udtName the user-defined type name for arrays (e.g., "_text" for text arrays)
   * @return the corresponding PostgresColumnType, or JSONB as fallback
   */
  public static PostgresColumnType fromPostgresType(String dataType, String udtName) {
    if (dataType == null) {
      return JSONB;
    }

    String lowerDataType = dataType.toLowerCase();

    // Handle array types first (indicated by udtName starting with underscore)
    if (udtName != null && udtName.startsWith("_")) {
      switch (udtName.toLowerCase()) {
        case "_text":
          return TEXT_ARRAY;
        default:
          return JSONB; // Unsupported array type, fallback to JSONB
      }
    }

    // Handle scalar types
    switch (lowerDataType) {
      case "text":
      case "varchar":
      case "character varying":
        return TEXT;

      case "bigint":
      case "int8":
        return BIGINT;

      case "double precision":
      case "float8":
        return DOUBLE_PRECISION;

      case "boolean":
      case "bool":
        return BOOLEAN;

      case "jsonb":
      case "json":
        return JSONB;

      default:
        return JSONB; // Unsupported type, fallback to existing JSONB behavior
    }
  }
}
