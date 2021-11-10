package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

public interface FilteringExpressionParser {
  Object parse(LogicalExpression expression);

  Object parse(RelationalExpression expression);
}
