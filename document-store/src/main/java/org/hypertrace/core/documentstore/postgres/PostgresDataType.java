package org.hypertrace.core.documentstore.postgres;

import java.util.Arrays;
import java.util.Set;

/**
 * Enumeration of PostgreSQL data types supported for first-class column processing.
 *
 * <p>Maps PostgreSQL native type names to our supported data types, enabling dynamic type-based
 * query generation and parser selection.
 */
public enum PostgresDataType {

  /** Text/string types - maps to String in Java */
  TEXT("text", "varchar"),

  /** Big integer type - maps to Long in Java */
  BIGINT("bigint", "int8"),

  /** Double precision floating point - maps to Double in Java */
  DOUBLE_PRECISION("double precision", "float8"),

  /** Boolean type - maps to Boolean in Java */
  BOOLEAN("boolean", "bool"),

  /** Text array type - maps to TextArray/String[] in Java */
  TEXT_ARRAY("_text"),

  /** Additional integer type for future support */
  INTEGER("integer", "int4"),

  /** JSONB type - fallback for complex types (excluded from first-class processing) */
  JSONB("jsonb");

  private final Set<String> postgresTypeNames;

  PostgresDataType(String... typeNames) {
    this.postgresTypeNames = Set.of(typeNames);
  }

  /**
   * Maps a PostgreSQL type name to our enum value.
   *
   * @param postgresTypeName the PostgreSQL type name from database metadata
   * @return the corresponding enum value, or JSONB as fallback
   */
  public static PostgresDataType fromPostgresTypeName(String postgresTypeName) {
    if (postgresTypeName == null) {
      return JSONB;
    }

    return Arrays.stream(values())
        .filter(type -> type.postgresTypeNames.contains(postgresTypeName.toLowerCase()))
        .findFirst()
        .orElse(JSONB);
  }

  /**
   * Checks if this data type should be treated as a first-class column.
   *
   * @return true if this type supports first-class column processing
   */
  public boolean isFirstClassType() {
    return this != JSONB;
  }

  /**
   * Gets the PostgreSQL type names that map to this enum value.
   *
   * @return set of PostgreSQL type names
   */
  public Set<String> getPostgresTypeNames() {
    return postgresTypeNames;
  }
}
