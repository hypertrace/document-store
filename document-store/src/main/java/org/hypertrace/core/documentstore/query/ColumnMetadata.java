package org.hypertrace.core.documentstore.query;

import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/**
 * Metadata about column types in a collection. Used to inform query parsers about native column
 * types vs JSONB fields.
 *
 * <p>Column types are represented as strings to avoid coupling with entity-service type enums.
 * Entity-service can pass AttributeKind.name() or ValueType.name() as the type string.
 *
 * <p>Example usage: <code>
 * ColumnMetadata metadata = ColumnMetadata.builder() .column("tags", "STRING_ARRAY")
 * .column("scores", "LONG_ARRAY") .column("attributes", "STRING_MAP") .build();
 * </code>
 */
@Value
@Builder
public class ColumnMetadata {

  @Singular
  Map<String, String> columns; // column_name -> type_string

  // Known native array type suffixes (from entity-service ValueType)
  private static final Set<String> NATIVE_ARRAY_TYPES = Set.of(
      "STRING_ARRAY",
      "LONG_ARRAY",
      "DOUBLE_ARRAY",
      "BOOLEAN_ARRAY"
  );

  // Known native scalar types (from entity-service ValueType)
  private static final Set<String> NATIVE_SCALAR_TYPES = Set.of(
      "STRING",
      "LONG",
      "DOUBLE",
      "BYTES",
      "BOOL",
      "TIMESTAMP"
  );

  // Known JSONB types (from entity-service ValueType)
  private static final Set<String> JSONB_TYPES = Set.of(
      "STRING_MAP"
  );

  /**
   * Get the column type string for a given column name.
   *
   * @param columnName the name of the column
   * @return the column type string, or null if not specified (defaults to JSONB storage)
   */
  public String getColumnType(String columnName) {
    return columns.get(columnName);
  }

  /**
   * Check if a column is a native array type.
   *
   * @param columnName the name of the column
   * @return true if the column is a native array type
   */
  public boolean isNativeArrayColumn(String columnName) {
    String type = getColumnType(columnName);
    if (type == null) {
      return false;
    }
    // Check exact match first, then check suffix pattern
    return NATIVE_ARRAY_TYPES.contains(type) || type.endsWith("_ARRAY");
  }

  /**
   * Check if a column is a native scalar type.
   *
   * @param columnName the name of the column
   * @return true if the column is a native scalar type
   */
  public boolean isNativeScalarColumn(String columnName) {
    String type = getColumnType(columnName);
    return type != null && NATIVE_SCALAR_TYPES.contains(type);
  }

  /**
   * Check if a column is a native type (array or scalar).
   *
   * @param columnName the name of the column
   * @return true if the column is a native type
   */
  public boolean isNativeColumn(String columnName) {
    return isNativeArrayColumn(columnName) || isNativeScalarColumn(columnName);
  }

  /**
   * Check if a column is explicitly a JSONB type (like STRING_MAP).
   *
   * @param columnName the name of the column
   * @return true if the column is explicitly marked as JSONB type
   */
  public boolean isJsonbColumn(String columnName) {
    String type = getColumnType(columnName);
    return type != null && JSONB_TYPES.contains(type);
  }

  /**
   * @return an empty ColumnMetadata instance (all columns default to JSONB storage)
   */
  public static ColumnMetadata empty() {
    return ColumnMetadata.builder().build();
  }
}
