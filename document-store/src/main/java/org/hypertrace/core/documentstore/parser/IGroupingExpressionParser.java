package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;

public interface IGroupingExpressionParser {
  void parse(FunctionExpression expression);

  void parse(IdentifierExpression expression);
}
