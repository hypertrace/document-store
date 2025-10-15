package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

@SuppressWarnings("unchecked")
public class AggregateExpressionChecker
    implements SelectTypeExpressionVisitor, SortTypeExpressionVisitor {

  @Override
  public Boolean visit(AggregateExpression expression) {
    return true;
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
    return false;
  }

  @Override
  public Boolean visit(IdentifierExpression expression) {
    return false;
  }

  @Override
  public Boolean visit(AliasedIdentifierExpression expression) {
    throw new UnsupportedOperationException("This operation is not supported");
  }
}
