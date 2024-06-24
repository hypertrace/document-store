package org.hypertrace.core.documentstore.mongo.query.parser;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;

public class AliasParser implements SelectTypeExpressionVisitor, SortTypeExpressionVisitor {

  @SuppressWarnings("unchecked")
  @Override
  public Optional<String> visit(AggregateExpression expression) {
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<String> visit(FunctionExpression expression) {
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<String> visit(IdentifierExpression expression) {
    return Optional.of(expression.getName());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<String> visit(ConstantExpression expression) {
    return Optional.empty();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Optional<String> visit(DocumentConstantExpression expression) {
    return Optional.empty();
  }
}
