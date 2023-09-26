package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.getUnsupportedOperationException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RootExpression;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MongoUnsupportedSelectTypeExpressionParser extends MongoSelectTypeExpressionParser {
  static final MongoUnsupportedSelectTypeExpressionParser INSTANCE =
      new MongoUnsupportedSelectTypeExpressionParser();

  @Override
  public <T> T visit(final AggregateExpression expression) {
    throw getUnsupportedOperationException(expression);
  }

  @Override
  public <T> T visit(final ConstantExpression expression) {
    throw getUnsupportedOperationException(expression);
  }

  @Override
  public <T> T visit(final DocumentConstantExpression expression) {
    throw getUnsupportedOperationException(expression);
  }

  @Override
  public <T> T visit(final FunctionExpression expression) {
    throw getUnsupportedOperationException(expression);
  }

  @Override
  public <T> T visit(final IdentifierExpression expression) {
    throw getUnsupportedOperationException(expression);
  }

  @Override
  public <T> T visit(final RootExpression expression) {
    throw getUnsupportedOperationException(expression);
  }
}
