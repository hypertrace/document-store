package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LIKE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.STARTS_WITH;

import com.google.common.collect.Maps;
import java.util.Map;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;

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

  private static final PostgresStandardRelationalFilterParser
      postgresStandardRelationalFilterParser = new PostgresStandardRelationalFilterParser();

  @Override
  public PostgresRelationalFilterParser parser(
      final RelationalExpression expression, final PostgresQueryParser postgresQueryParser) {

    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    RelationalOperator operator = expression.getOperator();
    // Transform EQ/NEQ to CONTAINS/NOT_CONTAINS for array fields with scalar RHS
    if (shouldConvertEqToContains(expression)) {
      operator = (expression.getOperator() == EQ) ? CONTAINS : NOT_CONTAINS;
    }

    if (operator == CONTAINS) {
      return expression.getLhs().accept(new PostgresContainsParserSelector(isFlatCollection));
    } else if (operator == IN) {
      return expression.getLhs().accept(new PostgresInParserSelector(isFlatCollection));
    } else if (operator == NOT_CONTAINS) {
      return parserMap.get(NOT_CONTAINS);
    }

    // For EQ/NEQ on array fields with array RHS, use specialized array equality parser
    if (shouldUseArrayEqualityParser(expression)) {
      return expression.getLhs().accept(new PostgresArrayEqualityParserSelector());
    }

    return parserMap.getOrDefault(expression.getOperator(), postgresStandardRelationalFilterParser);
  }

  /**
   * Determines if EQ/NEQ should be converted to CONTAINS/NOT_CONTAINS.
   *
   * <p>Conversion happens when:
   *
   * <ul>
   *   <li>Operator is EQ or NEQ
   *   <li>RHS is a SCALAR value (not an array/iterable)
   *   <li>LHS is a JsonIdentifierExpression with an array field type (STRING_ARRAY, NUMBER_ARRAY,
   *       etc.) OR
   *   <li>LHS is an ArrayIdentifierExpression with an array type (TEXT, BIGINT, etc.)
   * </ul>
   *
   * <p>If RHS is an array, we DO NOT convert - we want exact equality match (= operator), not
   * containment (@> operator).
   *
   * <p>This provides semantic equivalence: checking if an array contains a scalar value is more
   * intuitive than checking if the array equals the value.
   */
  private boolean shouldConvertEqToContains(final RelationalExpression expression) {
    if (expression.getOperator() != EQ && expression.getOperator() != NEQ) {
      return false;
    }

    // Check if RHS is an array/iterable - if so, don't convert (we want exact match)
    if (isArrayRhs(expression.getRhs())) {
      return false;
    }

    // RHS is scalar - check if LHS is an array field
    return isArrayField(expression.getLhs());
  }

  /**
   * Determines if we should use the specialized array equality parser.
   *
   * <p>Use this parser when:
   *
   * <ul>
   *   <li>Operator is EQ or NEQ
   *   <li>RHS is an array/iterable (for exact match)
   *   <li>LHS is either JsonIdentifierExpression with array type OR ArrayIdentifierExpression
   * </ul>
   */
  private boolean shouldUseArrayEqualityParser(final RelationalExpression expression) {
    if (expression.getOperator() != EQ && expression.getOperator() != NEQ) {
      return false;
    }
    // Check if RHS is an array/iterable AND LHS is an array field
    return isArrayRhs(expression.getRhs()) && isArrayField(expression.getLhs());
  }

  /** Checks if the RHS expression contains an array/iterable value. */
  private boolean isArrayRhs(final SelectTypeExpression rhs) {
    if (rhs instanceof ConstantExpression) {
      ConstantExpression constExpr = (ConstantExpression) rhs;
      return constExpr.getValue() instanceof Iterable;
    }
    return false;
  }

  /** Checks if the LHS expression is an array field. */
  private boolean isArrayField(final SelectTypeExpression lhs) {
    if (lhs instanceof JsonIdentifierExpression) {
      JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) lhs;
      return jsonExpr
          .getFieldType()
          .map(
              fieldType ->
                  fieldType == JsonFieldType.BOOLEAN_ARRAY
                      || fieldType == JsonFieldType.STRING_ARRAY
                      || fieldType == JsonFieldType.NUMBER_ARRAY
                      || fieldType == JsonFieldType.OBJECT_ARRAY)
          .orElse(false);
    }
    return lhs instanceof ArrayIdentifierExpression;
  }
}
