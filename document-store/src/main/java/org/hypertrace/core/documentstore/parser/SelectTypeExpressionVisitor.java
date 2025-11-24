package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;

public interface SelectTypeExpressionVisitor {
  <T> T visit(final AggregateExpression expression);

  <T> T visit(final ConstantExpression expression);

  <T> T visit(final DocumentConstantExpression expression);

  <T> T visit(final FunctionExpression expression);

  <T> T visit(final IdentifierExpression expression);

  <T> T visit(final AliasedIdentifierExpression expression);

  /**
   * Visit an ArrayIdentifierExpression. Default implementation delegates to
   * visit(IdentifierExpression) since ArrayIdentifierExpression extends IdentifierExpression.
   */
  default <T> T visit(final ArrayIdentifierExpression expression) {
    return visit((IdentifierExpression) expression);
  }

  /**
   * Visit a JsonIdentifierExpression. Default implementation delegates to
   * visit(IdentifierExpression) since JsonIdentifierExpression extends IdentifierExpression.
   */
  default <T> T visit(final JsonIdentifierExpression expression) {
    return visit((IdentifierExpression) expression);
  }
}
