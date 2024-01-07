package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.function.UnaryOperator;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

@AllArgsConstructor
public class PostgresArrayRelationalFilterParserGetter implements PostgresArrayFilterParserGetter {

  private final PostgresQueryParser postgresQueryParser;

  @Override
  public UnaryOperator<PostgresSelectTypeExpressionVisitor> getParser(
      final String arraySource, final String alias, final Object rhsValue) {
    // Override the base visitor,
    // pick the LHS field name (elements.inner),
    // replace it with alias (inner), and
    // optionally trim double quotes (TRIM('"' FROM inner::text))
    return baseVisitor ->
        new PostgresIdentifierTrimmingExpressionVisitor(
            new PostgresIdentifierReplacingExpressionVisitor(
                new PostgresIdentifierExpressionVisitor(postgresQueryParser), arraySource, alias),
            rhsValue);
  }
}
