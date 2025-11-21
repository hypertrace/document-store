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
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresContainsRelationalFilterParserNonJsonField;

class PostgresContainsParserSelector implements SelectTypeExpressionVisitor {

  private static final PostgresContainsRelationalFilterParserInterface jsonFieldContainsParser =
      new PostgresContainsRelationalFilterParser();
  private static final PostgresContainsRelationalFilterParserInterface nonJsonFieldContainsParser =
      new PostgresContainsRelationalFilterParserNonJsonField();

  private final boolean isFlatCollection;

  PostgresContainsParserSelector(boolean isFlatCollection) {
    this.isFlatCollection = isFlatCollection;
  }

  @Override
  public PostgresRelationalFilterParser visit(JsonIdentifierExpression expression) {
    return jsonFieldContainsParser;
  }

  @Override
  public PostgresRelationalFilterParser visit(ArrayIdentifierExpression expression) {
    return isFlatCollection ? nonJsonFieldContainsParser : jsonFieldContainsParser;
  }

  @Override
  public PostgresRelationalFilterParser visit(IdentifierExpression expression) {
    return isFlatCollection ? nonJsonFieldContainsParser : jsonFieldContainsParser;
  }

  @Override
  public PostgresRelationalFilterParser visit(AggregateExpression expression) {
    return isFlatCollection ? nonJsonFieldContainsParser : jsonFieldContainsParser;
  }

  @Override
  public PostgresRelationalFilterParser visit(ConstantExpression expression) {
    return isFlatCollection ? nonJsonFieldContainsParser : jsonFieldContainsParser;
  }

  @Override
  public PostgresRelationalFilterParser visit(DocumentConstantExpression expression) {
    return isFlatCollection ? nonJsonFieldContainsParser : jsonFieldContainsParser;
  }

  @Override
  public PostgresRelationalFilterParser visit(FunctionExpression expression) {
    return isFlatCollection ? nonJsonFieldContainsParser : jsonFieldContainsParser;
  }

  @Override
  public PostgresRelationalFilterParser visit(AliasedIdentifierExpression expression) {
    return isFlatCollection ? nonJsonFieldContainsParser : jsonFieldContainsParser;
  }
}
