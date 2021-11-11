package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public interface SortingExpressionParser {
  Object parse(final AggregateExpression expression);

  Object parse(final FunctionExpression expression);

  Object parse(final IdentifierExpression expression);
}
