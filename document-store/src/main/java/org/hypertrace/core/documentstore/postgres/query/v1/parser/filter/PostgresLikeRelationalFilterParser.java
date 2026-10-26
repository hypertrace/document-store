package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

class PostgresLikeRelationalFilterParser implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final Object parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    // Like operator is only applicable for string RHS. Hence, convert the RHS to string.
    // Postgres's `~*` is already an unanchored, case-insensitive regex search (not a full-string
    // match), so no ".*" wrapping is needed. If the RHS is a valid regex, pass it through
    // unchanged; otherwise escape POSIX ARE metacharacters so it is matched literally instead of
    // making Postgres's `~*` operator error out (see PostgresUtils#sanitizeRegexValue).
    final String rhsValue = PostgresUtils.sanitizeRegexValue(parsedRhs);
    context.getParamsBuilder().addObjectParam(rhsValue);

    // Case-insensitive regex search
    return String.format("%s ~* ?", parsedLhs);
  }
}
