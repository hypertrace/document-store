package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of NOT_IN operator for non-JSON fields (regular PostgreSQL typed columns). Uses
 * the PostgreSQL NOT IN operator for checking if a column value is not in a list of values.
 *
 * <p>This class is optimized for first-class typed columns rather than JSON document fields.
 */
public class PostgresNotInRelationalFilterParserNonJsonField
    implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    if (parsedRhs instanceof Iterable<?>) {
      // Build parameterized query with placeholders for each value
      Iterable<?> values = (Iterable<?>) parsedRhs;
      String placeholders =
          StreamSupport.stream(values.spliterator(), false)
              .map(
                  value -> {
                    context.getParamsBuilder().addObjectParam(value);
                    return "?";
                  })
              .collect(Collectors.joining(", "));

      return String.format("%s NOT IN (%s)", parsedLhs, placeholders);
    } else {
      // Single value case
      context.getParamsBuilder().addObjectParam(parsedRhs);
      return String.format("%s NOT IN (?)", parsedLhs);
    }
  }
}
