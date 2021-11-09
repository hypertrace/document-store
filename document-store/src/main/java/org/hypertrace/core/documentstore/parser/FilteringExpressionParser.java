package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.LogicalExpression;
import org.hypertrace.core.documentstore.expression.RelationalExpression;

public interface FilteringExpressionParser {
  Object parse(LogicalExpression expression);

  Object parse(RelationalExpression expression);
}
