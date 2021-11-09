package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;

public interface GroupingExpressionParser {
  Object parse(FunctionExpression expression);

  Object parse(IdentifierExpression expression);
}
