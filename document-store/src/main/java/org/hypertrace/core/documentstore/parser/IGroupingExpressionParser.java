package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;

public interface IGroupingExpressionParser {
  Object parse(FunctionExpression expression);

  Object parse(IdentifierExpression expression);
}
