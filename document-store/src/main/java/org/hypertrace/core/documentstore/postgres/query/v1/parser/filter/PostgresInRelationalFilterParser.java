package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;

class PostgresInRelationalFilterParser implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Iterable<Object> parsedRhs = expression.getRhs().accept(context.rhsParser());

    return prepareFilterStringForInOperator(parsedLhs, parsedRhs, context.getParamsBuilder());
  }

  private String prepareFilterStringForInOperator(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final Params.Builder paramsBuilder) {
    // In order to make the behaviour same as for Mongo, the "NOT_IN" operator would match if the
    // LHS and RHS have any intersection (i.e. non-empty intersection)
    return StreamSupport.stream(parsedRhs.spliterator(), false)
        .map(
            value -> {
              paramsBuilder.addObjectParam(value).addObjectParam(value);
              return String.format(
                  "((jsonb_typeof(to_jsonb(%s)) = 'array' AND to_jsonb(%s) @> jsonb_build_array(?)) OR (jsonb_build_array(%s) @> jsonb_build_array(?)))",
                  parsedLhs, parsedLhs, parsedLhs);
            })
        .collect(Collectors.joining(" OR ", "(", ")"));
  }
}
