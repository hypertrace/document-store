package org.hypertrace.core.documentstore.expression;

import org.hypertrace.core.documentstore.parser.ISelectingExpressionParser;

/**
 * A marker interface to represent that the expression is projectable and can be used in either the
 * SELECT clause of the query.
 */
public interface SelectingExpression {
  void parse(ISelectingExpressionParser parser);
}
