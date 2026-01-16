package org.hypertrace.core.documentstore.postgres.update;

import java.util.List;
import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;

/**
 * Context object containing all information needed to generate SQL for a single field update in
 * flat collections.
 */
@Value
@Builder
public class FlatUpdateContext {
  /** The column name in the database (e.g., "price", "props") */
  String columnName;

  /**
   * The nested path within a JSONB column, empty array for top-level columns. For example, for
   * "props.seller.name", columnName would be "props" and nestedPath would be ["seller", "name"].
   */
  String[] nestedPath;

  /** The PostgreSQL data type of the column */
  PostgresDataType columnType;

  /** The value to set/update */
  SubDocumentValue value;

  /** Accumulator for prepared statement parameters (mutable) */
  List<Object> params;

  /** Returns true if this is a top-level column update (no nested path) */
  public boolean isTopLevel() {
    return nestedPath == null || nestedPath.length == 0;
  }

  /** Returns true if the column is a JSONB type */
  public boolean isJsonbColumn() {
    return columnType == PostgresDataType.JSONB;
  }
}
