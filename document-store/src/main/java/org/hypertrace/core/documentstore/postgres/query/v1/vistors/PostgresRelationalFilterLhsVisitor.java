package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import lombok.Builder;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.Type;

public class PostgresRelationalFilterLhsVisitor extends PostgresSelectTypeExpressionVisitor {
  private final PostgresIdentifierExpressionVisitor identifierValueVisitor;
  private final PostgresSelectTypeExpressionVisitor lhsExpressionVisitor;

  public PostgresRelationalFilterLhsVisitor(
      final PostgresQueryParser postgresQueryParser, final Type valueType) {
    super(postgresQueryParser);
    identifierValueVisitor = new PostgresIdentifierExpressionVisitor(postgresQueryParser);
    lhsExpressionVisitor =
        new PostgresFunctionExpressionVisitor(
            new PostgresDataAccessorIdentifierExpressionVisitor(postgresQueryParser, valueType));
  }

  @Override
  public PostgresQueryParser getPostgresQueryParser() {
    return postgresQueryParser != null ? postgresQueryParser : baseVisitor.getPostgresQueryParser();
  }

  @SuppressWarnings("unchecked")
  @Override
  public RelationalFilterLhsParseResult visit(final FunctionExpression expression) {
    final String parsedExpression = expression.accept(lhsExpressionVisitor);
    return RelationalFilterLhsParseResult.builder()
        .fieldName(parsedExpression)
        .parsedExpression(parsedExpression)
        .build();
  }

  @SuppressWarnings("unchecked")
  @Override
  public RelationalFilterLhsParseResult visit(final IdentifierExpression expression) {
    return RelationalFilterLhsParseResult.builder()
        .fieldName(expression.accept(identifierValueVisitor))
        .parsedExpression(expression.accept(lhsExpressionVisitor))
        .build();
  }

  @Value
  @Builder
  static class RelationalFilterLhsParseResult {
    String fieldName;
    String parsedExpression;
  }
}
