package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public interface GroupingExpressionParser {
  Object parse(final FunctionExpression expression);

  Object parse(final IdentifierExpression expression);
}
