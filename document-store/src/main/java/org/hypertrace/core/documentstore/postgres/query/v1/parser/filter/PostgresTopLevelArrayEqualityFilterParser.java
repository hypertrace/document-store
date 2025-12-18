package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresTypeExtractor;

/**
 * Handles EQ/NEQ operations on top-level array columns when RHS is also an array, using exact
 * equality (=) instead of containment (@>).
 *
 * <p>Generates: {@code tags = ARRAY['hygiene','family-pack']::text[]}
 */
class PostgresTopLevelArrayEqualityFilterParser implements PostgresRelationalFilterParser {

  private static final PostgresStandardRelationalOperatorMapper mapper =
      new PostgresStandardRelationalOperatorMapper();

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    final String operator = mapper.getMapping(expression.getOperator(), parsedRhs);

    if (parsedRhs == null) {
      return String.format("%s %s NULL", parsedLhs, operator);
    }

    // Normalize to an Iterable
    Iterable<Object> values = normalizeToIterable(parsedRhs);

    // Add each value as an individual parameter
    String placeholders =
        StreamSupport.stream(values.spliterator(), false)
            .map(
                value -> {
                  context.getParamsBuilder().addObjectParam(value);
                  return "?";
                })
            .collect(Collectors.joining(", "));

    ArrayIdentifierExpression arrayExpr = (ArrayIdentifierExpression) expression.getLhs();
    String arrayTypeCast = arrayExpr.accept(PostgresTypeExtractor.arrayType());

    // Generate: tags = ARRAY[?, ?]::text[]
    if (arrayTypeCast != null) {
      return String.format("%s %s ARRAY[%s]::%s", parsedLhs, operator, placeholders, arrayTypeCast);
    } else {
      // Fallback to text[] cast
      return String.format("%s %s ARRAY[%s]::text[]", parsedLhs, operator, placeholders);
    }
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
