package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

@SuppressWarnings("unchecked")
public class GroupByAliasGetter implements GroupTypeExpressionVisitor {

  @Override
  public String visit(FunctionExpression expression) {
    return null;
  }

  @Override
  public String visit(IdentifierExpression expression) {
    return expression.getName();
  }
}
