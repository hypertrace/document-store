package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.function.UnaryOperator;

public interface PostgresArrayFilterParserGetter {
  UnaryOperator<PostgresSelectTypeExpressionVisitor> getParser(
      final String arraySource, final String alias, final Object rhsValue);
}
