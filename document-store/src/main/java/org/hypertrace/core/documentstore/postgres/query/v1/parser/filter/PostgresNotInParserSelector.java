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
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserArrayField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserNonJsonField;

class PostgresNotInParserSelector implements SelectTypeExpressionVisitor {

  private static final PostgresInRelationalFilterParserInterface jsonFieldInFilterParser =
      new PostgresInRelationalFilterParser();
  private static final PostgresInRelationalFilterParserInterface scalarFieldInFilterParser =
      new PostgresInRelationalFilterParserNonJsonField();
  private static final PostgresInRelationalFilterParserInterface arrayFieldInFilterParser =
      new PostgresInRelationalFilterParserArrayField();

  private final boolean isFlatCollection;

  PostgresNotInParserSelector(boolean isFlatCollection) {
    this.isFlatCollection = isFlatCollection;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(JsonIdentifierExpression expression) {
    return jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(ArrayIdentifierExpression expression) {
    // Array fields with explicit ArrayIdentifierExpression use optimized array parser
    return isFlatCollection ? arrayFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(IdentifierExpression expression) {
    // IdentifierExpression: use scalar parser in flat, JSON parser in nested
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(AggregateExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(ConstantExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(DocumentConstantExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @Override
  public PostgresInRelationalFilterParserInterface visit(FunctionExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }

  @SuppressWarnings("unchecked")
  @Override
  public PostgresInRelationalFilterParserInterface visit(AliasedIdentifierExpression expression) {
    return isFlatCollection ? scalarFieldInFilterParser : jsonFieldInFilterParser;
  }
}
