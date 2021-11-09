package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.AggregateExpression;
import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;

public interface ISortingExpressionParser {
  void parse(AggregateExpression expression);

  void parse(FunctionExpression expression);

  void parse(IdentifierExpression expression);
}
