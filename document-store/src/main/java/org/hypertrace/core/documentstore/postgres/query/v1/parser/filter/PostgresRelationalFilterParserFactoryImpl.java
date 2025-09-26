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
import org.hypertrace.core.documentstore.postgres.PostgresColumnRegistry;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresContainsRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresExistsRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresInRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresLikeRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresNotContainsRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresNotExistsRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresNotInRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresStandardRelationalFilterParserNonJsonField;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresStartsWithRelationalFilterParserNonJsonField;

public class PostgresRelationalFilterParserFactoryImpl
    implements PostgresRelationalFilterParserFactory {
  // JSON-based parsers for JSONB document fields (fallback)
  private static final Map<RelationalOperator, PostgresRelationalFilterParser> jsonParserMap =
      Maps.immutableEnumMap(
          Map.ofEntries(
              entry(NOT_CONTAINS, new PostgresNotContainsRelationalFilterParser()),
              entry(EXISTS, new PostgresExistsRelationalFilterParser()),
              entry(NOT_EXISTS, new PostgresNotExistsRelationalFilterParser()),
              entry(NOT_IN, new PostgresNotInRelationalFilterParser()),
              entry(LIKE, new PostgresLikeRelationalFilterParser()),
              entry(STARTS_WITH, new PostgresStartsWithRelationalFilterParser())));

  // Non-JSON parsers for first-class typed columns
  private static final Map<RelationalOperator, PostgresRelationalFilterParser> nonJsonParserMap =
      Maps.immutableEnumMap(
          Map.ofEntries(
              entry(NOT_CONTAINS, new PostgresNotContainsRelationalFilterParserNonJsonField()),
              entry(EXISTS, new PostgresExistsRelationalFilterParserNonJsonField()),
              entry(NOT_EXISTS, new PostgresNotExistsRelationalFilterParserNonJsonField()),
              entry(NOT_IN, new PostgresNotInRelationalFilterParserNonJsonField()),
              entry(LIKE, new PostgresLikeRelationalFilterParserNonJsonField()),
              entry(STARTS_WITH, new PostgresStartsWithRelationalFilterParserNonJsonField())));

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

  // Standard parsers for EQ, NEQ, GT, LT, GTE, LTE operations
  private static final PostgresStandardRelationalFilterParser
      postgresStandardRelationalFilterParser = new PostgresStandardRelationalFilterParser();
  private static final PostgresStandardRelationalFilterParserNonJsonField
      postgresStandardRelationalFilterParserNonJsonField =
          new PostgresStandardRelationalFilterParserNonJsonField();

  @Override
  public PostgresRelationalFilterParser parser(
      final RelationalExpression expression, final PostgresQueryParser postgresQueryParser) {

    // Extract field name from LHS of the expression to check registry
    String fieldName = extractFieldName(expression);

    // Get the column registry from the query parser context
    PostgresColumnRegistry registry = postgresQueryParser.getColumnRegistry();

    // Determine if this field should use first-class column processing - support both old and new
    // patterns
    boolean isFirstClassField = false;

    // NEW: Registry-based check (preferred)
    if (registry != null && registry.isFirstClassColumn(fieldName)) {
      isFirstClassField = true;
    }

    // OLD: flatStructureCollection check (for backward compatibility)
    String flatStructureCollection = postgresQueryParser.getFlatStructureCollectionName();
    if (flatStructureCollection != null
        && flatStructureCollection.equals(
            postgresQueryParser.getTableIdentifier().getTableName())) {
      isFirstClassField = true;
    }

    // Select appropriate parser based on field type and operator
    if (expression.getOperator() == CONTAINS) {
      return isFirstClassField ? nonJsonFieldContainsParser : jsonFieldContainsParser;
    } else if (expression.getOperator() == IN) {
      return isFirstClassField ? nonJsonFieldInFilterParser : jsonFieldInFilterParser;
    }

    // For other operators, choose between JSON and non-JSON parser maps
    if (isFirstClassField) {
      return nonJsonParserMap.getOrDefault(
          expression.getOperator(), postgresStandardRelationalFilterParserNonJsonField);
    } else {
      return jsonParserMap.getOrDefault(
          expression.getOperator(), postgresStandardRelationalFilterParser);
    }
  }

  /**
   * Extracts the field name from the LHS of a relational expression. This is used to check if the
   * field is a first-class typed column.
   */
  private String extractFieldName(RelationalExpression expression) {
    // For now, we'll use a simple approach - this may need refinement based on expression structure
    // The LHS is typically a FieldExpression that contains the field name
    String lhsString = expression.getLhs().toString();

    // Handle simple field names (most common case)
    if (lhsString != null && !lhsString.contains(".") && !lhsString.contains("(")) {
      return lhsString;
    }

    // For complex expressions, we may not be able to extract a simple field name
    // In this case, fall back to JSON processing
    return null;
  }
}
