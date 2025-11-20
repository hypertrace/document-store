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
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresContainsRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserArrayField;
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
  private static final PostgresInRelationalFilterParserInterface scalarFieldInFilterParser =
      new PostgresInRelationalFilterParserNonJsonField();
  private static final PostgresInRelationalFilterParserInterface arrayFieldInFilterParser =
      new PostgresInRelationalFilterParserArrayField();

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

    // Check if LHS is a JSON field (JSONB column access)
    boolean isJsonField = expression.getLhs() instanceof JsonIdentifierExpression;

    // Check if LHS is an array field (native PostgreSQL array column)
    boolean isArrayField = expression.getLhs() instanceof ArrayIdentifierExpression;

    // Check if the collection type is flat or nested
    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    boolean useJsonParser = !isFlatCollection || isJsonField;

    if (expression.getOperator() == CONTAINS) {
      return useJsonParser ? jsonFieldContainsParser : nonJsonFieldContainsParser;
    } else if (expression.getOperator() == IN) {
      // For IN operator, we need to distinguish between:
      // 1. JSON fields (JSONB) -> use JSONB containment logic
      // 2. Array fields (native arrays) -> use array overlap operator (&&)
      // 3. Scalar fields -> use simple IN clause
      if (useJsonParser) {
        return jsonFieldInFilterParser;
      } else if (isArrayField) {
        return arrayFieldInFilterParser;
      } else {
        return scalarFieldInFilterParser;
      }
    }

    return parserMap.getOrDefault(expression.getOperator(), postgresStandardRelationalFilterParser);
  }
}
