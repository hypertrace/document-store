package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LIKE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.STARTS_WITH;

import com.google.common.collect.Maps;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser.PostgresRelationalFilterContext;

public class PostgresRelationalFilterParserFactoryImpl
    implements PostgresRelationalFilterParserFactory {
  private static final Map<RelationalOperator, PostgresRelationalFilterParser> parserMap =
      Maps.immutableEnumMap(
          Map.ofEntries(
              entry(CONTAINS, new PostgresContainsRelationalFilterParser()),
              entry(NOT_CONTAINS, new PostgresNotContainsRelationalFilterParser()),
              entry(EXISTS, new PostgresExistsRelationalFilterParser()),
              entry(NOT_EXISTS, new PostgresNotExistsRelationalFilterParser()),
              entry(IN, new PostgresInRelationalFilterParser()),
              entry(NOT_IN, new PostgresNotInRelationalFilterParser()),
              entry(LIKE, new PostgresLikeRelationalFilterParser()),
              entry(STARTS_WITH, new PostgresStartsWithRelationalFilterParser())));
  private static final PostgresStandardRelationalFilterParser
      postgresStandardRelationalFilterParser = new PostgresStandardRelationalFilterParser();

  @Override
  public PostgresRelationalFilterParser parser(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    return parserMap.getOrDefault(expression.getOperator(), postgresStandardRelationalFilterParser);
  }
}
