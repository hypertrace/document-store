package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;

public class MongoStandardRelationalOperatorMapping {
  private static final Map<RelationalOperator, String> mapping =
      Maps.immutableEnumMap(
          Map.ofEntries(
              entry(EQ, "$eq"),
              entry(NEQ, "$ne"),
              entry(GT, "$gt"),
              entry(LT, "$lt"),
              entry(GTE, "$gte"),
              entry(LTE, "$lte"),
              entry(IN, "$in"),
              entry(NOT_IN, "$nin")));

  public String getOperator(final RelationalOperator operator) {
    return Optional.ofNullable(mapping.get(operator))
        .orElseThrow(() -> new UnsupportedOperationException("Unsupported operator: " + operator));
  }
}
