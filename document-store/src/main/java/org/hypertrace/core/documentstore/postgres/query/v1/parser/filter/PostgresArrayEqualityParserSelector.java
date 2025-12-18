package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

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
 * Selects the appropriate array equality parser based on the LHS expression type.
 *
 * <p>For JsonIdentifierExpression: uses JSONB array equality parser
 *
 * <p>For ArrayIdentifierExpression: uses top-level array equality parser
 */
class PostgresArrayEqualityParserSelector implements SelectTypeExpressionVisitor {

  private static final PostgresRelationalFilterParser jsonArrayEqualityParser =
      new PostgresJsonArrayEqualityFilterParser();
  private static final PostgresRelationalFilterParser topLevelArrayEqualityParser =
      new PostgresTopLevelArrayEqualityFilterParser();
  private static final PostgresRelationalFilterParser standardParser =
      new PostgresStandardRelationalFilterParser();

  @Override
  public PostgresRelationalFilterParser visit(JsonIdentifierExpression expression) {
    return jsonArrayEqualityParser;
  }

  @Override
  public PostgresRelationalFilterParser visit(ArrayIdentifierExpression expression) {
    return topLevelArrayEqualityParser;
  }

  @Override
  public <T> T visit(IdentifierExpression expression) {
    return (T) standardParser;
  }

  @Override
  public <T> T visit(AggregateExpression expression) {
    return (T) standardParser;
  }

  @Override
  public <T> T visit(ConstantExpression expression) {
    return (T) standardParser;
  }

  @Override
  public <T> T visit(DocumentConstantExpression expression) {
    return (T) standardParser;
  }

  @Override
  public <T> T visit(FunctionExpression expression) {
    return (T) standardParser;
  }

  @Override
  public <T> T visit(AliasedIdentifierExpression expression) {
    return (T) standardParser;
  }
}
