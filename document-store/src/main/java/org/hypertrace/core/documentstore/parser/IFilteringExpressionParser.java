package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.LogicalExpression;
import org.hypertrace.core.documentstore.expression.RelationalExpression;

public interface IFilteringExpressionParser {
  void parse(LogicalExpression expression);

  void parse(RelationalExpression expression);
}
