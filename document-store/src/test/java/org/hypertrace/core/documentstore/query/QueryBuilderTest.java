package org.hypertrace.core.documentstore.query;

import static java.util.Collections.emptyList;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.query.Query.QueryBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
class QueryBuilderTest {

  private QueryBuilder queryBuilder;

  @BeforeEach
  void setUp() {
    queryBuilder = Query.builder();
  }

  @Test
  void testSetAndClearSelections() {
    final Query queryWithSelections =
        queryBuilder.addSelection(ConstantExpression.of("Something")).build();
    assertEquals(1, queryWithSelections.getSelections().size());

    final Query queryWithoutSelections = queryBuilder.setSelections(emptyList()).build();
    assertEquals(0, queryWithoutSelections.getSelections().size());
  }

  @Test
  void testSetAndClearSorts() {
    final Query queryWithSorts =
        queryBuilder.addSort(IdentifierExpression.of("Something"), DESC).build();
    assertEquals(1, queryWithSorts.getSorts().size());

    final Query queryWithoutSorts = queryBuilder.setSorts(emptyList()).build();
    assertEquals(0, queryWithoutSorts.getSorts().size());
  }

  @Test
  void testSetAndClearAggregations() {
    final Query queryWithAggregations =
        queryBuilder.addAggregation(IdentifierExpression.of("Something")).build();
    assertEquals(1, queryWithAggregations.getAggregations().size());

    final Query queryWithoutAggregations = queryBuilder.setAggregations(emptyList()).build();
    assertEquals(0, queryWithoutAggregations.getAggregations().size());
  }

  @Test
  void testSetAndClearFromClauses() {
    final Query queryWithFromClauses =
        queryBuilder
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("Something"), false))
            .build();
    assertEquals(1, queryWithFromClauses.getFromTypeExpressions().size());

    final Query queryWithoutFromClauses = queryBuilder.setFromClauses(emptyList()).build();
    assertEquals(0, queryWithoutFromClauses.getFromTypeExpressions().size());
  }
}
