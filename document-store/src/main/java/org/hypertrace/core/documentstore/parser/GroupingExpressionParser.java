package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;

public interface GroupingExpressionParser {
  Object parse(FunctionExpression expression);

  Object parse(IdentifierExpression expression);
}
