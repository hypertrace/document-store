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
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
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

    // Determine if we should use JSON-specific parsers based on:
    // 1. Collection type (nested collections store entire document as JSONB)
    // 2. Field type (flat collections may have JSONB columns for nested data)
    boolean useJsonParser = shouldUseJsonParser(expression, postgresQueryParser);

    if (expression.getOperator() == CONTAINS) {
      return useJsonParser ? jsonFieldContainsParser : nonJsonFieldContainsParser;
    } else if (expression.getOperator() == IN) {
      return useJsonParser ? jsonFieldInFilterParser : nonJsonFieldInFilterParser;
    }

    return parserMap.getOrDefault(expression.getOperator(), postgresStandardRelationalFilterParser);
  }

  /**
   * Determines if JSON-specific parser should be used based on collection type and field type.
   *
   * @param expression the relational expression to evaluate
   * @param postgresQueryParser the query parser context
   * @return true if JSON parser should be used, false for standard SQL parser
   */
  private boolean shouldUseJsonParser(
      final RelationalExpression expression, final PostgresQueryParser postgresQueryParser) {
    // Check if the collection type is flat or nested
    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    // Check if LHS is a JSON field (JSONB column access)
    boolean isJsonField = expression.getLhs() instanceof JsonIdentifierExpression;

    // Use JSON parsers for:
    // 1. Nested collections (entire document is JSONB) - !isFlatCollection
    // 2. JSON fields within flat collections - isJsonField
    return !isFlatCollection || isJsonField;
  }
}
