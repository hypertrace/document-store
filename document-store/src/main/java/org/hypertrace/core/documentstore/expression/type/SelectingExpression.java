package org.hypertrace.core.documentstore.expression.type;

import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;

/** An interface to represent that the expression can be used in the SELECT clause of the query. */
public interface SelectingExpression {
  Object parse(SelectingExpressionParser parser);
}
