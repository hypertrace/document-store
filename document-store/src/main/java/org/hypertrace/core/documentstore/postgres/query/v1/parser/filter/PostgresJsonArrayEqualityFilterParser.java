package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

/**
 * Handles EQ/NEQ operations on JSONB array fields when RHS is also an array, using exact equality
 * (=) instead of containment (@>).
 *
 * <p>Generates: {@code props->'source-loc' = '["hygiene","family-pack"]'::jsonb}
 */
class PostgresJsonArrayEqualityFilterParser implements PostgresRelationalFilterParser {

  private static final PostgresStandardRelationalOperatorMapper mapper =
      new PostgresStandardRelationalOperatorMapper();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    final String operator = mapper.getMapping(expression.getOperator(), parsedRhs);

    if (parsedRhs == null) {
      return String.format("%s %s NULL", parsedLhs, operator);
    }

    // Convert the array to a JSONB string representation
    try {
      String jsonbValue;
      if (parsedRhs instanceof Iterable) {
        jsonbValue = OBJECT_MAPPER.writeValueAsString(parsedRhs);
      } else {
        jsonbValue = String.valueOf(parsedRhs);
      }
      context.getParamsBuilder().addObjectParam(jsonbValue);
      return String.format("%s %s ?::jsonb", parsedLhs, operator);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize RHS array to JSON", e);
    }
  }
}
