package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

public interface FilteringExpressionParser {
  Object parse(final LogicalExpression expression);

  Object parse(final RelationalExpression expression);
}
