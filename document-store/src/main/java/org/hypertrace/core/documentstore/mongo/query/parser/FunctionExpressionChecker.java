package org.hypertrace.core.documentstore.mongo.query.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;

@SuppressWarnings("unchecked")
public class FunctionExpressionChecker
    implements SelectTypeExpressionVisitor, SortTypeExpressionVisitor {

  @Override
  public Boolean visit(AggregateExpression expression) {
    return false;
  }

  @Override
  public Boolean visit(ConstantExpression expression) {
    return false;
  }

  @Override
  public Boolean visit(DocumentConstantExpression expression) {
    return false;
  }

  @Override
  public Boolean visit(FunctionExpression expression) {
    return true;
  }

  @Override
  public Boolean visit(IdentifierExpression expression) {
    return false;
  }
}
