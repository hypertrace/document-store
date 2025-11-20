package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresInRelationalFilterParserInterface;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of PostgresInRelationalFilterParserInterface for handling IN operations on array
 * fields (non-JSON array columns), using the PostgreSQL array overlap operator (&&).
 *
 * <p>For array fields like "tags", the IN operator semantics are: "does the array contain ANY of
 * the provided values?" This is implemented using the PostgreSQL array overlap operator (&&).
 *
 * <p>Example: tags IN ('hygiene', 'premium') translates to: tags && ARRAY['hygiene',
 * 'premium']::text[]
 */
public class PostgresInRelationalFilterParserArrayField
    implements PostgresInRelationalFilterParserInterface {

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Iterable<Object> parsedRhs = expression.getRhs().accept(context.rhsParser());

    return prepareFilterStringForInOperator(parsedLhs, parsedRhs, context.getParamsBuilder());
  }

  private String prepareFilterStringForInOperator(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final Params.Builder paramsBuilder) {

    String placeholders =
        StreamSupport.stream(parsedRhs.spliterator(), false)
            .map(
                value -> {
                  paramsBuilder.addObjectParam(value);
                  return "?";
                })
            .collect(Collectors.joining(", "));

    // Use array overlap operator for array fields
    // Cast both LHS and RHS to text[] to avoid type mismatch issues
    // (e.g., text[] vs varchar[], integer[] vs text[], etc.)
    return String.format("%s::text[] && ARRAY[%s]::text[]", parsedLhs, placeholders);
  }
}
