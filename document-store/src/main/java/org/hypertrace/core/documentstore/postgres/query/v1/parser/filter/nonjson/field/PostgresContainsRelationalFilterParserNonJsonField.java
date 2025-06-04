package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import java.util.Collection;
import java.util.Collections;
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

    Object normalizedRhs = normalizeValue(parsedRhs);
    context.getParamsBuilder().addObjectParam(normalizedRhs);

    return String.format("%s @> ARRAY[?]::text[]", parsedLhs);
  }

  private Object normalizeValue(final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof Collection) {
      return value;
    } else {
      return Collections.singletonList(value);
    }
  }
}
