package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayType;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

class PostgresArrayTypeExtractor implements SelectTypeExpressionVisitor {

  public PostgresArrayTypeExtractor() {}

  @Override
  public String visit(ArrayIdentifierExpression expression) {
    return expression.getArrayType().map(ArrayType::getPostgresType).orElse(null);
  }

  @Override
  public String visit(JsonIdentifierExpression expression) {
    return null; // JSON fields don't have array type
  }

  @Override
  public String visit(IdentifierExpression expression) {
    return null; // Regular identifiers don't have array type (backward compat fallback)
  }

  @Override
  public String visit(AggregateExpression expression) {
    return null;
  }

  @Override
  public String visit(ConstantExpression expression) {
    return null;
  }

  @Override
  public String visit(DocumentConstantExpression expression) {
    return null;
  }

  @Override
  public String visit(FunctionExpression expression) {
    return null;
  }

  @Override
  public String visit(AliasedIdentifierExpression expression) {
    return null;
  }
}
