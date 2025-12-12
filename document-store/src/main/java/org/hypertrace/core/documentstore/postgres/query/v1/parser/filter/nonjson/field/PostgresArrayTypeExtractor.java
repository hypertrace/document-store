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
 * Visitor to extract PostgreSQL array type information from {@link ArrayIdentifierExpression}.
 *
 * <p>This visitor is specifically designed to work ONLY with {@link ArrayIdentifierExpression}. Any
 * other expression type will throw {@link UnsupportedOperationException} to catch programming
 * errors early.
 *
 * <p>Returns:
 *
 * <ul>
 *   <li>The PostgreSQL array type string (e.g., "text[]", "integer[]")
 *   <li>{@code null} if {@link ArrayIdentifierExpression} is used without an explicit type
 * </ul>
 */
public class PostgresArrayTypeExtractor implements SelectTypeExpressionVisitor {

  public PostgresArrayTypeExtractor() {}

  @Override
  public String visit(ArrayIdentifierExpression expression) {
    return expression.getPostgresArrayTypeString().orElse(null);
  }

  @Override
  public String visit(JsonIdentifierExpression expression) {
    throw unsupportedExpression("JsonIdentifierExpression");
  }

  @Override
  public String visit(IdentifierExpression expression) {
    throw new UnsupportedOperationException(
        "PostgresArrayTypeExtractor should only be used with ArrayIdentifierExpression. "
            + "Use IdentifierExpression only for scalar fields, not arrays.");
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
        "PostgresArrayTypeExtractor should only be used with ArrayIdentifierExpression, not "
            + expressionType);
  }
}
