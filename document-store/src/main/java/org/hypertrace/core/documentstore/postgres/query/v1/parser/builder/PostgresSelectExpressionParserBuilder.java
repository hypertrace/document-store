package org.hypertrace.core.documentstore.postgres.query.v1.parser.builder;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;

public interface PostgresSelectExpressionParserBuilder {
  PostgresSelectTypeExpressionVisitor build(final RelationalExpression expression);
}
