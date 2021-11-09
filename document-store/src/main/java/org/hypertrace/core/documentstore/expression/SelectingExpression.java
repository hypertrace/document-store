package org.hypertrace.core.documentstore.expression;

import org.hypertrace.core.documentstore.parser.ISelectingExpressionParser;

/**
 * An interface to represent that the expression can be used in the
 * SELECT clause of the query.
 */
public interface SelectingExpression {
  void parse(ISelectingExpressionParser parser);
}
