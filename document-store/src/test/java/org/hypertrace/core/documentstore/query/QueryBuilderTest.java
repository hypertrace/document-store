package org.hypertrace.core.documentstore.query;

import static java.util.Collections.emptyList;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
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
  public void testQuerySimple() {
    Query query = Query.builder().build();
    assertEquals("SELECT * " + "FROM <implicit_collection>", query.toString());
  }

  @Test
  public void testQueryWithSelection() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("id"))
            .addSelection(IdentifierExpression.of("fname"), "name")
            .build();
    assertEquals("SELECT `id`, `fname` AS name " + "FROM <implicit_collection>", query.toString());
  }

  @Test
  public void testQueryWithFilter() {
    Query query =
        Query.builder()
            .setFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("percentage"), GT, ConstantExpression.of(90)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("class"), EQ, ConstantExpression.of("XII")))
                    .build())
            .build();

    assertEquals(
        "SELECT * "
            + "FROM <implicit_collection> "
            + "WHERE (`percentage` > 90) AND (`class` = 'XII')",
        query.toString());
  }

  @Test
  public void testQueryWithSorting() {
    Query query =
        Query.builder()
            .addSort(IdentifierExpression.of("marks"), DESC)
            .addSort(IdentifierExpression.of("name"), SortOrder.ASC)
            .build();

    assertEquals(
        "SELECT * " + "FROM <implicit_collection> " + "ORDER BY `marks` DESC, `name` ASC",
        query.toString());
  }

  @Test
  public void testQueryWithPagination() {
    Query query =
        Query.builder().setPagination(Pagination.builder().limit(10).offset(50).build()).build();

    assertEquals(
        "SELECT * " + "FROM <implicit_collection> " + "LIMIT 10 " + "OFFSET 50", query.toString());
  }

  @Test
  public void testQueryWithAllClauses() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("id"))
            .addSelection(IdentifierExpression.of("fname"), "name")
            .addSort(IdentifierExpression.of("marks"), DESC)
            .addSort(IdentifierExpression.of("name"), SortOrder.ASC)
            .setPagination(Pagination.builder().offset(50).limit(10).build())
            .setFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("percentage"), GTE, ConstantExpression.of(90)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("class"), NEQ, ConstantExpression.of("XII")))
                    .build())
            .build();

    assertEquals(
        "SELECT `id`, `fname` AS name "
            + "FROM <implicit_collection> "
            + "WHERE (`percentage` >= 90) AND (`class` != 'XII') "
            + "ORDER BY `marks` DESC, `name` ASC "
            + "LIMIT 10 "
            + "OFFSET 50",
        query.toString());
  }

  @Test
  public void testSimpleAggregate() {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
            .build();

    assertEquals("SELECT COUNT(1) AS total " + "FROM <implicit_collection>", query.toString());
  }

  @Test
  public void testFieldCount() {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("path")), "total")
            .build();

    assertEquals("SELECT COUNT(`path`) AS total " + "FROM <implicit_collection>", query.toString());
  }

  @Test
  public void testAggregateWithProjections() {
    Query query =
        Query.builder()
            .addSelections(
                List.of(
                    SelectionSpec.of(
                        AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total"),
                    SelectionSpec.of(IdentifierExpression.of("name"))))
            .addAggregation(IdentifierExpression.of("name"))
            .build();

    assertEquals(
        "SELECT COUNT(1) AS total, `name` " + "FROM <implicit_collection> " + "GROUP BY `name`",
        query.toString());
  }

  @Test
  public void testAggregateWithMultiLevelGrouping() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("name"))
            .addSelection(AggregateExpression.of(MIN, IdentifierExpression.of("rank")), "topper")
            .addAggregations(
                List.of(IdentifierExpression.of("name"), IdentifierExpression.of("class")))
            .build();

    assertEquals(
        "SELECT `name`, MIN(`rank`) AS topper "
            + "FROM <implicit_collection> "
            + "GROUP BY `name`, `class`",
        query.toString());
  }

  @Test
  public void testAggregateWithFilter() {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(SUM, IdentifierExpression.of("marks")), "total")
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("section"),
                    IN,
                    ConstantExpression.ofStrings(List.of("A", "B", "C"))))
            .build();

    assertEquals(
        "SELECT SUM(`marks`) AS total "
            + "FROM <implicit_collection> "
            + "WHERE `section` IN [A, B, C]",
        query.toString());
  }

  @Test
  public void testAggregateWithGroupingFilter() {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(
                    SUM,
                    FunctionExpression.builder()
                        .operand(IdentifierExpression.of("price"))
                        .operator(MULTIPLY)
                        .operand(IdentifierExpression.of("quantity"))
                        .build()),
                "total")
            .addAggregation(IdentifierExpression.of("order"))
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("total"),
                    NOT_IN,
                    ConstantExpression.ofNumbers(List.of(100, 200, 500))))
            .build();

    assertEquals(
        "SELECT SUM(MULTIPLY(`price`, `quantity`)) AS total "
            + "FROM <implicit_collection> "
            + "GROUP BY `order` "
            + "HAVING `total` NOT IN [100, 200, 500]",
        query.toString());
  }

  @Test
  public void testAggregateWithSorting() {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(
                    AVG, AggregateExpression.of(MAX, IdentifierExpression.of("mark"))),
                "averageHighScore")
            .addAggregation(IdentifierExpression.of("section"))
            .addSorts(
                List.of(
                    SortingSpec.of(IdentifierExpression.of("averageHighScore"), DESC),
                    SortingSpec.of(IdentifierExpression.of("section"), ASC)))
            .build();

    assertEquals(
        "SELECT AVG(MAX(`mark`)) AS averageHighScore "
            + "FROM <implicit_collection> "
            + "GROUP BY `section` "
            + "ORDER BY `averageHighScore` DESC, `section` ASC",
        query.toString());
  }

  @Test
  public void testAggregateWithPagination() {
    Query query =
        Query.builder()
            .addAggregation(IdentifierExpression.of("student"))
            .setPagination(Pagination.builder().offset(0).limit(10).build())
            .build();

    assertEquals(
        "SELECT * "
            + "FROM <implicit_collection> "
            + "GROUP BY `student` "
            + "LIMIT 10 "
            + "OFFSET 0",
        query.toString());
  }

  @Test
  public void testGetDistinctCount() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("class"), LTE, ConstantExpression.of(10)))
            .addAggregation(IdentifierExpression.of("class"))
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("section")),
                "section_count")
            .build();

    assertEquals(
        "SELECT DISTINCT_COUNT(`section`) AS section_count "
            + "FROM <implicit_collection> "
            + "WHERE `class` <= 10 "
            + "GROUP BY `class`",
        query.toString());
  }

  @Test
  public void testUnwindAndGroup() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("class"), LTE, ConstantExpression.of(10)))
            .addAggregation(IdentifierExpression.of("class.students.courses"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("class.students"), true))
            .addFromClause(
                UnnestExpression.of(IdentifierExpression.of("class.students.courses"), true))
            .build();

    assertEquals(
        "SELECT * "
            + "FROM <implicit_collection>, "
            + "UNNEST(`class.students`, true, null), "
            + "UNNEST(`class.students.courses`, true, null) "
            + "WHERE `class` <= 10 "
            + "GROUP BY `class.students.courses`",
        query.toString());
  }

  @Test
  void testQueryWithFunctionalLhsInRelationalFilter() {
    final Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .setFilter(
                RelationalExpression.of(
                    FunctionExpression.builder()
                        .operator(MULTIPLY)
                        .operand(IdentifierExpression.of("quantity"))
                        .operand(IdentifierExpression.of("price"))
                        .build(),
                    GT,
                    ConstantExpression.of(50)))
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    assertEquals(
        "SELECT `item` "
            + "FROM <implicit_collection> "
            + "WHERE MULTIPLY(`quantity`, `price`) > 50 "
            + "ORDER BY `item` DESC",
        query.toString());
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
