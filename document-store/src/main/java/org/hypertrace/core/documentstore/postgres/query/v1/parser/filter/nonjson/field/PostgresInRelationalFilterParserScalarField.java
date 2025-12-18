package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresInRelationalFilterParserInterface;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

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

    // Extract type from expression metadata
    String sqlType = expression.getLhs().accept(PostgresTypeExtractor.scalarType());

    // If type is specified, use optimized ANY(ARRAY[]) syntax
    // Otherwise, fall back to traditional IN (?, ?, ?) for backward compatibility
    if (sqlType != null) {
      return prepareFilterStringForInOperatorWithArray(
          parsedLhs, parsedRhs, sqlType, context.getParamsBuilder());
    } else {
      return prepareFilterStringForInOperatorFallback(
          parsedLhs, parsedRhs, context.getParamsBuilder());
    }
  }

  /** Optimized IN using = ANY(?) with typed array parameter. */
  private String prepareFilterStringForInOperatorWithArray(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final String sqlType,
      final Params.Builder paramsBuilder) {

    Object[] values = StreamSupport.stream(parsedRhs.spliterator(), false).toArray();

    if (values.length == 0) {
      // Evaluates to FALSE
      return "1 = 0";
    }

    paramsBuilder.addArrayParam(values, sqlType);

    return String.format("%s = ANY(?)", parsedLhs);
  }

  /**
   * Fallback IN using traditional IN (?, ?, ?) syntax for backward compatibility when type
   * information is not available.
   */
  private String prepareFilterStringForInOperatorFallback(
      final String parsedLhs,
      final Iterable<Object> parsedRhs,
      final Params.Builder paramsBuilder) {

    String collect =
        StreamSupport.stream(parsedRhs.spliterator(), false)
            .map(
                val -> {
                  paramsBuilder.addObjectParam(val);
                  return "?";
                })
            .collect(Collectors.joining(", "));

    if (collect.isEmpty()) {
      return "1 = 0";
    }

    return String.format("%s IN (%s)", parsedLhs, collect);
  }
}
