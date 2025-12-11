package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresInRelationalFilterParserInterface;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

/**
 * Implementation of PostgresInRelationalFilterParserInterface for handling IN operations on
 * first-class fields (non-JSON columns), using the optimized = ANY(ARRAY[]) syntax.
 */
public class PostgresInRelationalFilterParserScalarField
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

    Object[] values = StreamSupport.stream(parsedRhs.spliterator(), false).toArray();

    paramsBuilder.addArrayParam(values, PostgresUtils.inferSqlTypeFromValue(values));

    return String.format("%s = ANY(?)", parsedLhs);
  }
}
