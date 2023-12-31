package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresConstantExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.vistors.PostgresSelectTypeExpressionVisitor;

public interface PostgresRelationalFilterParser {
  String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context);

  @Value
  @Builder
  @Accessors(fluent = true)
  class PostgresRelationalFilterContext {
    PostgresSelectTypeExpressionVisitor lhsParser;

    @Default
    PostgresSelectTypeExpressionVisitor rhsParser = new PostgresConstantExpressionVisitor();

    @Delegate PostgresQueryParser postgresQueryParser;
  }
}
