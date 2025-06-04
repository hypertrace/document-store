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
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresContainsRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserNonJsonField;

public class PostgresRelationalFilterParserFactoryImpl
    implements PostgresRelationalFilterParserFactory {
  private static final Map<RelationalOperator, PostgresRelationalFilterParser> parserMap =
      Maps.immutableEnumMap(
          Map.ofEntries(
              // CONTAINS is conditionally chosen between JSON and non-JSON versions
              entry(NOT_CONTAINS, new PostgresNotContainsRelationalFilterParser()),
              entry(EXISTS, new PostgresExistsRelationalFilterParser()),
              entry(NOT_EXISTS, new PostgresNotExistsRelationalFilterParser()),
              // IN  are conditionally chosen between JSON and non-JSON versions
              entry(NOT_IN, new PostgresNotInRelationalFilterParser()),
              entry(LIKE, new PostgresLikeRelationalFilterParser()),
              entry(STARTS_WITH, new PostgresStartsWithRelationalFilterParser())));

  // IN filter parsers
  private static final PostgresInRelationalFilterParserInterface jsonFieldInFilterParser =
      new PostgresInRelationalFilterParser();
  private static final PostgresInRelationalFilterParserInterface nonJsonFieldInFilterParser =
      new PostgresInRelationalFilterParserNonJsonField();

  // CONTAINS parser implementations
  private static final PostgresContainsRelationalFilterParserInterface jsonFieldContainsParser =
      new PostgresContainsRelationalFilterParser();
  private static final PostgresContainsRelationalFilterParserInterface nonJsonFieldContainsParser =
      new PostgresContainsRelationalFilterParserNonJsonField();

  private static final PostgresStandardRelationalFilterParser
      postgresStandardRelationalFilterParser = new PostgresStandardRelationalFilterParser();

  @Override
  public PostgresRelationalFilterParser parser(
      final RelationalExpression expression, final PostgresQueryParser postgresQueryParser) {

    boolean isFirstClassField =
        postgresQueryParser
            .getFlatStructureCollectionName()
            .map(name -> name.equals(postgresQueryParser.getTableIdentifier().getTableName()))
            .orElse(false);

    if (expression.getOperator() == CONTAINS) {
      return isFirstClassField ? nonJsonFieldContainsParser : jsonFieldContainsParser;
    } else if (expression.getOperator() == IN) {
      return isFirstClassField ? nonJsonFieldInFilterParser : jsonFieldInFilterParser;
    }

    return parserMap.getOrDefault(expression.getOperator(), postgresStandardRelationalFilterParser);
  }
}
