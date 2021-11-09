package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.AggregateExpression;
import org.hypertrace.core.documentstore.expression.ConstantExpression;
import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;

public interface SelectingExpressionParser {
  Object parse(AggregateExpression expression);

  Object parse(ConstantExpression expression);

  Object parse(FunctionExpression expression);

  Object parse(IdentifierExpression expression);
}
