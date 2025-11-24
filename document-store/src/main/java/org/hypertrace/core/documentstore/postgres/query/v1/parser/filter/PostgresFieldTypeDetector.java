package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

/**
 * Visitor to detect the category of a field expression for array-aware SQL generation.
 *
 * <p>Categorizes fields into three types:
 *
 * <ul>
 *   <li><b>SCALAR:</b> Regular fields and JSON primitives (strings, numbers, booleans, objects)
 *   <li><b>POSTGRES_ARRAY:</b> Native PostgreSQL arrays (text[], integer[], boolean[], etc.)
 *   <li><b>JSONB_ARRAY:</b> Arrays inside JSONB columns with JsonFieldType annotation
 * </ul>
 *
 * <p>This categorization is used by EXISTS/NOT_EXISTS parsers to generate appropriate SQL:
 *
 * <ul>
 *   <li>SCALAR: {@code IS NOT NULL / IS NULL}
 *   <li>POSTGRES_ARRAY: {@code IS NOT NULL AND cardinality(...) > 0}
 *   <li>JSONB_ARRAY: {@code IS NOT NULL AND jsonb_array_length(...) > 0}
 * </ul>
 */
class PostgresFieldTypeDetector implements SelectTypeExpressionVisitor {

  /** Field category for determining appropriate SQL generation strategy */
  enum FieldCategory {
    SCALAR, // Regular fields and JSON primitives
    ARRAY, // Native PostgreSQL arrays (text[], int[], etc.)
    JSONB_ARRAY // Arrays inside JSONB columns
  }

  @Override
  public FieldCategory visit(ArrayIdentifierExpression expression) {
    return FieldCategory.ARRAY;
  }

  @Override
  public FieldCategory visit(JsonIdentifierExpression expression) {
    return expression
        .getFieldType()
        .filter(
            type ->
                type == JsonFieldType.STRING_ARRAY
                    || type == JsonFieldType.NUMBER_ARRAY
                    || type == JsonFieldType.BOOLEAN_ARRAY
                    || type == JsonFieldType.OBJECT_ARRAY)
        .map(type -> FieldCategory.JSONB_ARRAY)
        .orElse(FieldCategory.SCALAR);
  }

  @Override
  public FieldCategory visit(IdentifierExpression expression) {
    return FieldCategory.SCALAR;
  }

  @Override
  public FieldCategory visit(AggregateExpression expression) {
    return FieldCategory.SCALAR;
  }

  @Override
  public FieldCategory visit(ConstantExpression expression) {
    return FieldCategory.SCALAR;
  }

  @Override
  public FieldCategory visit(DocumentConstantExpression expression) {
    return FieldCategory.SCALAR;
  }

  @Override
  public FieldCategory visit(FunctionExpression expression) {
    return FieldCategory.SCALAR;
  }

  @Override
  public FieldCategory visit(AliasedIdentifierExpression expression) {
    return FieldCategory.SCALAR;
  }
}
