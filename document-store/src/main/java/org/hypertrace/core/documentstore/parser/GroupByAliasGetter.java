package org.hypertrace.core.documentstore.parser;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;

@SuppressWarnings("unchecked")
public class GroupByAliasGetter implements GroupTypeExpressionVisitor {

  @Override
  public Optional<String> visit(FunctionExpression expression) {
    return Optional.empty();
  }

  @Override
  public Optional<String> visit(IdentifierExpression expression) {
    return Optional.of(expression.getName());
  }

  @Override
  public Optional<String> visit(JsonIdentifierExpression expression) {
    return Optional.of(expression.getName());
  }
}
