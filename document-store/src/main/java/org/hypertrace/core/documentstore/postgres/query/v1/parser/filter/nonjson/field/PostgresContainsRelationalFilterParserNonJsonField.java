package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresContainsRelationalFilterParserInterface;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of CONTAINS operator for non-JSON fields (regular PostgreSQL arrays). Uses the
 * PostgreSQL array containment operator (@>) for checking if one array contains another.
 *
 * <p>This class is optimized for first-class array columns rather than JSON document fields.
 */
public class PostgresContainsRelationalFilterParserNonJsonField
    implements PostgresContainsRelationalFilterParserInterface {

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    // Normalize to an Iterable (single value becomes a singleton list)
    Iterable<Object> values = normalizeToIterable(parsedRhs);

    // Add each value as an individual parameter (same as IN operator)
    String placeholders =
        StreamSupport.stream(values.spliterator(), false)
            .map(
                value -> {
                  context.getParamsBuilder().addObjectParam(value);
                  return "?";
                })
            .collect(Collectors.joining(", "));

    // Check if this field has been unnested - if so, it's now a scalar, not an array
    // For ArrayIdentifierExpression, get the field name
    if (expression.getLhs() instanceof ArrayIdentifierExpression) {
      ArrayIdentifierExpression arrayExpr = (ArrayIdentifierExpression) expression.getLhs();
      String fieldName = arrayExpr.getName();
      if (context.getPgColumnNames().containsKey(fieldName)) {
        // Field is unnested - each element is now a scalar
        // Use scalar IN operator: the scalar must be IN the set of values we're looking for
        return String.format("%s IN (%s)", parsedLhs, placeholders);
      }
    }

    // Field is NOT unnested - use array containment operator
    String arrayType = expression.getLhs().accept(PostgresTypeExtractor.arrayType());
    // Fallback to text[] if type is unknown
    String typeCast = (arrayType != null) ? arrayType : "text[]";

    return String.format("%s @> ARRAY[%s]::%s", parsedLhs, placeholders, typeCast);
  }

  private Iterable<Object> normalizeToIterable(final Object value) {
    if (value == null) {
      return Collections.emptyList();
    } else if (value instanceof Iterable) {
      return (Iterable<Object>) value;
    } else {
      return Collections.singletonList(value);
    }
  }
}
