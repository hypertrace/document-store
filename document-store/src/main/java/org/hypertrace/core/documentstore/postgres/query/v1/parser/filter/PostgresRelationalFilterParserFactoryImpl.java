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
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresContainsRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.registry.PostgresColumnRegistry;

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

    // Extract field name from the left-hand side of the expression
    String fieldName = extractFieldName(expression);
    boolean isFirstClassField = determineIfFirstClassField(fieldName, postgresQueryParser);

    if (expression.getOperator() == CONTAINS) {
      return isFirstClassField ? nonJsonFieldContainsParser : jsonFieldContainsParser;
    } else if (expression.getOperator() == IN) {
      return isFirstClassField ? nonJsonFieldInFilterParser : jsonFieldInFilterParser;
    }

    return parserMap.getOrDefault(expression.getOperator(), postgresStandardRelationalFilterParser);
  }

  /**
   * Extracts the field name from the left-hand side of a RelationalExpression. Currently supports
   * IdentifierExpression; returns null for other types.
   */
  private String extractFieldName(RelationalExpression expression) {
    if (expression.getLhs() instanceof IdentifierExpression) {
      return ((IdentifierExpression) expression.getLhs()).getName();
    }
    // For other expression types (functions, etc.), we can't easily extract a field name
    return null;
  }

  /** Determines if a field is a first-class column using registry-based lookup with fallback. */
  private boolean determineIfFirstClassField(
      String fieldName, PostgresQueryParser postgresQueryParser) {
    PostgresColumnRegistry registry = postgresQueryParser.getColumnRegistry();

    // Use registry-based type resolution if available
    if (registry != null && fieldName != null) {
      return registry.isFirstClassColumn(fieldName);
    } else {
      // Fallback to flatStructureCollection for backward compatibility
      String flatStructureCollection = postgresQueryParser.getFlatStructureCollectionName();
      return flatStructureCollection != null
          && flatStructureCollection.equals(
              postgresQueryParser.getTableIdentifier().getTableName());
    }
  }
}
