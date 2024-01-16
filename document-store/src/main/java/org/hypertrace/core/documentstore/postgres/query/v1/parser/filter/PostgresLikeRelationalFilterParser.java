package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;

class PostgresLikeRelationalFilterParser implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression, final PostgresRelationalFilterContext context) {
    final Object parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    // Append ".*" at beginning and end of value to do a regex
    // Like operator is only applicable for string RHS. Hence, convert the RHS to string
    final String rhsValue = ".*" + parsedRhs + ".*";
    context.getParamsBuilder().addObjectParam(rhsValue);

    // Case-insensitive regex search
    return String.format("%s ~* ?", parsedLhs);
  }
}
