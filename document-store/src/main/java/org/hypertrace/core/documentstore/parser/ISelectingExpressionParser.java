package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.AggregateExpression;
import org.hypertrace.core.documentstore.expression.ConstantExpression;
import org.hypertrace.core.documentstore.expression.FunctionExpression;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;

public interface ISelectingExpressionParser {
  void parse(AggregateExpression expression);

  void parse(ConstantExpression expression);

  void parse(FunctionExpression expression);

  void parse(IdentifierExpression expression);
}
