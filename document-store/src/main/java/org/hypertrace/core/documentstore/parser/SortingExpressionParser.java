package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public interface SortingExpressionParser {
  Object parse(AggregateExpression expression);

  Object parse(FunctionExpression expression);

  Object parse(IdentifierExpression expression);
}
