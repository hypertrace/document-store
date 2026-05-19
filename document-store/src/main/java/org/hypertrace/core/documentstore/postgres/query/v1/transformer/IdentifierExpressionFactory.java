package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import org.hypertrace.core.documentstore.commons.ColumnMetadata;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

/**
 * Builds typed {@link IdentifierExpression}s (or {@link ArrayIdentifierExpression}s for array
 * columns) from {@link ColumnMetadata}. Used by the legacy-query transformers so that downstream
 * Postgres parsers can emit array-aware SQL for typed and array-typed columns:
 *
 * <ul>
 *   <li>{@code IN} &rarr; {@code col && ?} with a typed {@code java.sql.Array} parameter (for array
 *       columns) or {@code col = ANY(?)} (for scalar typed columns)
 *   <li>{@code CONTAINS}&rarr; {@code col @> ARRAY[?]::<elem>[]} (typed cast)
 *   <li>{@code NEQ} &rarr; routed through the top-level array equality parser when the LHS reaches
 *       the parser as an {@link ArrayIdentifierExpression}
 * </ul>
 *
 * <p>Falls back to an untyped {@code IdentifierExpression} when type information is missing,
 * UNSPECIFIED, or {@code JSON} (JSON columns are handled separately via {@code
 * JsonIdentifierExpression} paths), preserving backward-compatible behavior for callers that have
 * not yet wired column type metadata into their {@code SchemaRegistry}.
 */
final class IdentifierExpressionFactory {

  private IdentifierExpressionFactory() {}

  /**
   * Creates a typed identifier expression for a directly-resolved column.
   *
   * @param name the column name to use as the identifier
   * @param column the column metadata describing the column's canonical type and array-ness; if
   *     {@code null} or carrying no usable type information, returns an untyped identifier
   * @return an {@link ArrayIdentifierExpression} for array columns with a known element type, a
   *     typed {@link IdentifierExpression} for scalar columns with a known type, or an untyped
   *     {@link IdentifierExpression} otherwise
   */
  static IdentifierExpression createIdentifierFromColumn(
      final String name, final ColumnMetadata column) {
    if (column == null) {
      return IdentifierExpression.of(name);
    }
    final DataType type = column.getCanonicalType();
    // JSON columns are accessed via JsonIdentifierExpression paths elsewhere; do not wrap them as
    // typed scalar identifiers here.
    if (type == null || type == DataType.UNSPECIFIED || type == DataType.JSON) {
      return IdentifierExpression.of(name);
    }
    return column.isArray() ? createTypedArray(name, type) : createTypedScalar(name, type);
  }

  private static IdentifierExpression createTypedScalar(final String name, final DataType type) {
    switch (type) {
      case STRING:
        return IdentifierExpression.ofString(name);
      case INTEGER:
        return IdentifierExpression.ofInt(name);
      case LONG:
        return IdentifierExpression.ofLong(name);
      case FLOAT:
        return IdentifierExpression.ofFloat(name);
      case DOUBLE:
        return IdentifierExpression.ofDouble(name);
      case BOOLEAN:
        return IdentifierExpression.ofBoolean(name);
      case TIMESTAMPTZ:
        return IdentifierExpression.ofTimestampTz(name);
      case DATE:
        return IdentifierExpression.ofDate(name);
      default:
        return IdentifierExpression.of(name);
    }
  }

  private static ArrayIdentifierExpression createTypedArray(
      final String name, final DataType elementType) {
    switch (elementType) {
      case STRING:
        return ArrayIdentifierExpression.ofStrings(name);
      case INTEGER:
        return ArrayIdentifierExpression.ofInts(name);
      case LONG:
        return ArrayIdentifierExpression.ofLongs(name);
      case FLOAT:
        return ArrayIdentifierExpression.ofFloats(name);
      case DOUBLE:
        return ArrayIdentifierExpression.ofDoubles(name);
      case BOOLEAN:
        return ArrayIdentifierExpression.ofBooleans(name);
      case TIMESTAMPTZ:
        return ArrayIdentifierExpression.ofTimestampsTz(name);
      case DATE:
        return ArrayIdentifierExpression.ofDates(name);
      default:
        return ArrayIdentifierExpression.of(name);
    }
  }
}
