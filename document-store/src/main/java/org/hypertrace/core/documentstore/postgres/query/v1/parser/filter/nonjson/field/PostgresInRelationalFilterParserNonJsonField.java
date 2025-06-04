package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresInRelationalFilterParserInterface;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of PostgresInRelationalFilterParserInterface for handling IN operations on
 * first-class fields (non-JSON columns), using the more efficient array-based syntax.
 */
public class PostgresInRelationalFilterParserNonJsonField
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

    return String.format("%s && ARRAY[%s]::text[]", parsedLhs, placeholders);
  }
}
