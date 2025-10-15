package org.hypertrace.core.documentstore.parser;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
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

  <T> T visit(final JsonIdentifierExpression expression);

  <T> T visit(final AliasedIdentifierExpression expression);
}
