package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

public interface PostgresRelationalFilterParserFactory {
  PostgresRelationalFilterParser parser(final RelationalExpression expression);
}
