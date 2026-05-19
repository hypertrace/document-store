package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import org.hypertrace.core.documentstore.commons.ColumnMetadata;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

final class IdentifierExpressionFactory {

  private IdentifierExpressionFactory() {}

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
