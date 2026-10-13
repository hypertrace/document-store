package org.hypertrace.core.documentstore.parser;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

@SuppressWarnings("unchecked")
public class GroupByAliasGetter implements GroupTypeExpressionVisitor {

  @Override
  public Optional<String> visit(FunctionExpression expression) {
    String alias = expression.getAlias();
    if (alias == null || alias.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(alias);
  }

  @Override
  public Optional<String> visit(IdentifierExpression expression) {
    return Optional.of(expression.getName());
  }
}
