package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

/**
 * Visitor to extract PostgreSQL type information from identifier expressions.
 *
 * <p>Supports two extraction modes:
 *
 * <ul>
 *   <li><b>Scalar type:</b> Returns type names (e.g., "int4", "text") for use with {@code
 *       Connection.createArrayOf()}
 *   <li><b>Array type:</b> Returns array type strings (e.g., "int4[]", "text[]") for SQL type
 *       casting
 * </ul>
 *
 * <p>Returns {@code null} if the expression has UNSPECIFIED type.
 */
public class PostgresTypeExtractor implements SelectTypeExpressionVisitor {

  private final boolean extractArrayType;

  private PostgresTypeExtractor(boolean extractArrayType) {
    this.extractArrayType = extractArrayType;
  }

  /**
   * Creates an extractor that returns scalar SQL type names.
   *
   * @return A type extractor returning types like "int4", "float8", "text"
   */
  public static PostgresTypeExtractor scalarType() {
    return new PostgresTypeExtractor(false);
  }

  /**
   * Creates an extractor that returns array SQL type strings for SQL type casting.
   *
   * @return A type extractor returning array types like "int4[]", "text[]"
   */
  public static PostgresTypeExtractor arrayType() {
    return new PostgresTypeExtractor(true);
  }

  @Override
  public String visit(IdentifierExpression expression) {
    PostgresDataType pgType = PostgresDataType.fromDataType(expression.getDataType());
    if (pgType == PostgresDataType.UNKNOWN) {
      return null;
    }
    return extractArrayType ? pgType.getArraySqlType() : pgType.getSqlType();
  }

  @Override
  public String visit(ArrayIdentifierExpression expression) {
    PostgresDataType pgType = PostgresDataType.fromDataType(expression.getElementDataType());
    if (pgType == PostgresDataType.UNKNOWN) {
      return null;
    }
    return extractArrayType ? pgType.getArraySqlType() : pgType.getSqlType();
  }

  @Override
  public String visit(JsonIdentifierExpression expression) {
    throw unsupportedExpression("JsonIdentifierExpression");
  }

  @Override
  public String visit(AggregateExpression expression) {
    throw unsupportedExpression("AggregateExpression");
  }

  @Override
  public String visit(ConstantExpression expression) {
    throw unsupportedExpression("ConstantExpression");
  }

  @Override
  public String visit(DocumentConstantExpression expression) {
    throw unsupportedExpression("DocumentConstantExpression");
  }

  @Override
  public String visit(FunctionExpression expression) {
    throw unsupportedExpression("FunctionExpression");
  }

  @Override
  public String visit(AliasedIdentifierExpression expression) {
    throw unsupportedExpression("AliasedIdentifierExpression");
  }

  private static UnsupportedOperationException unsupportedExpression(String expressionType) {
    return new UnsupportedOperationException(
        "PostgresTypeExtractor should only be used with IdentifierExpression or "
            + "ArrayIdentifierExpression, not "
            + expressionType);
  }
}
