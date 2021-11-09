package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.AggregateExpression;
import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;

public interface SortingExpressionParser {
  Object parse(AggregateExpression expression);

  Object parse(FunctionExpression expression);

  Object parse(IdentifierExpression expression);
}
