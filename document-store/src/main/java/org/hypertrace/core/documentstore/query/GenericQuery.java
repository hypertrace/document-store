package org.hypertrace.core.documentstore.query;

import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.Filterable;
import org.hypertrace.core.documentstore.expression.Groupable;
import org.hypertrace.core.documentstore.expression.Projectable;

/**
 * A generic query definition that supports expressions. Note that this class is a more general
 * version of {@link org.hypertrace.core.documentstore.Query}
 *
 * <p>Example: SELECT col4, SUM(col5) AS total FROM <collection> WHERE col1 < 7 AND col2 != col3
 * GROUP BY col4, col6 HAVING SUM(col5) >= 100 ORDER BY col7+col8 DESC OFFSET 5 LIMIT 10
 *
 * <p>can be built as
 *
 * <p>GenericQuery query = GenericQuery.builder() .projection(LiteralExpression.of("col4"))
 * .projection( AggregateExpression.of(SUM, LiteralExpression.of("col5")), "total")
 * .whereFilter(LogicalExpression.of( RelationalExpression.of( LiteralExpression.of("col1"), LT,
 * ConstantExpression.of(7)), AND, RelationalExpression.of( LiteralExpression.of("col2"), NEQ,
 * LiteralExpression.of("col3")))) .groupBy(LiteralExpression.of("col4"))
 * .groupBy(LiteralExpression.of("col6")) .havingFilter( RelationalExpression.of(
 * AggregateExpression.of(SUM, LiteralExpression.of("col5")), GTE, ConstantExpression.of(100)))
 * .orderBy( GenericOrderBy.of( ArithmeticExpression.builder()
 * .operand(LiteralExpression.of("col7")) .operator(ADD)
 * .operand(LiteralExpression.of("col8")).build(), false)) .offset(5) .limit(10) .build();
 */
@Value
@Builder
public class GenericQuery {
  List<Projection> projections;
  Filterable whereFilter;

  @Singular List<Groupable> groupBys;
  Filterable havingFilter;

  @Singular List<GenericOrderBy> orderBys;

  int offset;
  int limit;

  public static class GenericQueryBuilder {
    private List<Projection> projections;

    public GenericQueryBuilder projection(Projectable projectable) {
      if (projections == null) {
        projections = new ArrayList<>();
      }

      projections.add(Projection.of(projectable));
      return this;
    }

    public GenericQueryBuilder projection(Projectable projectable, String alias) {
      if (projections == null) {
        projections = new ArrayList<>();
      }

      projections.add(Projection.of(projectable, alias));
      return this;
    }
  }
}
