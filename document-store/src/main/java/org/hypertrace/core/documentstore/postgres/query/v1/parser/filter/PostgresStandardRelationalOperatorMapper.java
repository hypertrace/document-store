package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;

class PostgresStandardRelationalOperatorMapper {
  private static final Map<RelationalOperator, String> mapping =
      Maps.immutableEnumMap(
          Map.ofEntries(
              entry(EQ, "="),
              entry(NEQ, "!="),
              entry(GT, ">"),
              entry(LT, "<"),
              entry(GTE, ">="),
              entry(LTE, "<=")));

  String getMapping(final RelationalOperator operator) {
    return Optional.ofNullable(mapping.get(operator))
        .orElseThrow(() -> new UnsupportedOperationException("Unsupported operator: " + operator));
  }
}
