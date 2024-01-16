package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

public interface PostgresWrappingFilterVisitorProvider {
  PostgresSelectTypeExpressionVisitor getForRelational(
      final PostgresSelectTypeExpressionVisitor baseVisitor, final SelectTypeExpression rhs);

  PostgresSelectTypeExpressionVisitor getForNonRelational(
      final PostgresSelectTypeExpressionVisitor baseVisitor);
}
