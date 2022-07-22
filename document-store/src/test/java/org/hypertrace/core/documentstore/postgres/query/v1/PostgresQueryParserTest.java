package org.hypertrace.core.documentstore.postgres.query.v1;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PostgresQueryParserTest {
  private static final String TEST_COLLECTION = "testCollection";

  @Test
  void testParseSimpleFilter() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT * FROM testCollection "
            + "WHERE document->'quantity' IS NULL OR CAST (document->>'quantity' AS NUMERIC) != ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(10, params.getObjectParams().get(1));
  }

  @Test
  void testFilterWithNestedFiled() {
    Query query =
        Query.builder()
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("props.seller.address.city"),
                            EQ,
                            ConstantExpression.of("Kolkata")))
                    .build())
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT * FROM testCollection "
            + "WHERE (CAST (document->>'quantity' AS NUMERIC) > ?) "
            + "AND (document->'props'->'seller'->'address'->>'city' = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(5, params.getObjectParams().get(1));
    Assertions.assertEquals("Kolkata", params.getObjectParams().get(2));
  }

  @Test
  void testFilterWithLogicalExpressionAnd() {
    Query query =
        Query.builder()
            .setFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GTE, ConstantExpression.of(5)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), LTE, ConstantExpression.of(10)))
                    .build())
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT * FROM testCollection WHERE (CAST (document->>'quantity' AS NUMERIC) >= ?) "
            + "AND (CAST (document->>'quantity' AS NUMERIC) <= ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(5, params.getObjectParams().get(1));
    Assertions.assertEquals(10, params.getObjectParams().get(2));
  }

  @Test
  void testFilterWithLogicalExpressionOr() {
    Query query =
        Query.builder()
            .setFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GTE, ConstantExpression.of(5)))
                    .operator(OR)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), LTE, ConstantExpression.of(10)))
                    .build())
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT * FROM testCollection WHERE (CAST (document->>'quantity' AS NUMERIC) >= ?) "
            + "OR (CAST (document->>'quantity' AS NUMERIC) <= ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(5, params.getObjectParams().get(1));
    Assertions.assertEquals(10, params.getObjectParams().get(2));
  }

  @Test
  void testFilterWithLogicalExpressionAndOr() {
    Query query =
        Query.builder()
            .setFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("price"), GTE, ConstantExpression.of(5)))
                    .operator(AND)
                    .operand(
                        LogicalExpression.builder()
                            .operand(
                                RelationalExpression.of(
                                    IdentifierExpression.of("quantity"),
                                    GTE,
                                    ConstantExpression.of(5)))
                            .operator(OR)
                            .operand(
                                RelationalExpression.of(
                                    IdentifierExpression.of("quantity"),
                                    LTE,
                                    ConstantExpression.of(10)))
                            .build())
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT * FROM testCollection WHERE (CAST (document->>'price' AS NUMERIC) >= ?) "
            + "AND ((CAST (document->>'quantity' AS NUMERIC) >= ?) "
            + "OR (CAST (document->>'quantity' AS NUMERIC) <= ?))",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(5, params.getObjectParams().get(1));
    Assertions.assertEquals(5, params.getObjectParams().get(2));
    Assertions.assertEquals(10, params.getObjectParams().get(3));
  }

  @Test
  void testBasicSelectionExpression() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT document->'item' AS item, document->'price' AS price FROM testCollection", sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(0, params.getObjectParams().size());
  }

  @Test
  void testFunctionalSelectionExpression() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(
                FunctionExpression.builder()
                    .operand(IdentifierExpression.of("price"))
                    .operator(MULTIPLY)
                    .operand(IdentifierExpression.of("quantity"))
                    .build(),
                "total")
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT document->'item' AS item, "
            + "CAST (document->>'price' AS NUMERIC) * CAST (document->>'quantity' AS NUMERIC) AS \"total\" "
            + "FROM testCollection",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(0, params.getObjectParams().size());
  }

  @Test
  void testFunctionalSelectionExpressionWithNestedField() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("props.brand"))
            .addSelection(IdentifierExpression.of("props.seller.name"))
            .addSelection(
                FunctionExpression.builder()
                    .operand(IdentifierExpression.of("price"))
                    .operator(MULTIPLY)
                    .operand(IdentifierExpression.of("quantity"))
                    .build(),
                "total")
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT document->'item' AS item, "
            + "document->'props'->'brand' AS props_dot_brand, "
            + "document->'props'->'seller'->'name' AS props_dot_seller_dot_name, "
            + "CAST (document->>'price' AS NUMERIC) * CAST (document->>'quantity' AS NUMERIC) AS \"total\" "
            + "FROM testCollection",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(0, params.getObjectParams().size());
  }

  @Test
  void testFunctionalSelectionExpressionWithNestedFieldWithAlias() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("props.brand"), "props_band")
            .addSelection(IdentifierExpression.of("props.seller.name"), "props_seller_name")
            .addSelection(
                FunctionExpression.builder()
                    .operand(IdentifierExpression.of("price"))
                    .operator(MULTIPLY)
                    .operand(IdentifierExpression.of("quantity"))
                    .build(),
                "total")
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT document->'item' AS item, "
            + "document->'props'->'brand' AS \"props_band\", "
            + "document->'props'->'seller'->'name' AS \"props_seller_name\", "
            + "CAST (document->>'price' AS NUMERIC) * CAST (document->>'quantity' AS NUMERIC) AS \"total\" "
            + "FROM testCollection",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(0, params.getObjectParams().size());
  }

  @Test
  void testAggregationExpression() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), EQ, ConstantExpression.of(10)))
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(
                AggregateExpression.of(AVG, IdentifierExpression.of("quantity")), "qty_avg")
            .addSelection(
                AggregateExpression.of(COUNT, IdentifierExpression.of("quantity")), "qty_count")
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_distinct_count")
            .addSelection(
                AggregateExpression.of(SUM, IdentifierExpression.of("quantity")), "qty_sum")
            .addSelection(
                AggregateExpression.of(MIN, IdentifierExpression.of("quantity")), "qty_min")
            .addSelection(
                AggregateExpression.of(MAX, IdentifierExpression.of("quantity")), "qty_max")
            .addAggregation(IdentifierExpression.of("item"))
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT document->'item' AS item, "
            + "AVG( CAST (document->>'quantity' AS NUMERIC) ) AS \"qty_avg\", "
            + "COUNT( document->>'quantity' ) AS \"qty_count\", "
            + "COUNT(DISTINCT document->>'quantity' ) AS \"qty_distinct_count\", "
            + "SUM( CAST (document->>'quantity' AS NUMERIC) ) AS \"qty_sum\", "
            + "MIN( CAST (document->>'quantity' AS NUMERIC) ) AS \"qty_min\", "
            + "MAX( CAST (document->>'quantity' AS NUMERIC) ) AS \"qty_max\" "
            + "FROM testCollection WHERE CAST (document->>'price' AS NUMERIC) = ? "
            + "GROUP BY document->'item'",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(1, params.getObjectParams().size());
    Assertions.assertEquals(10, params.getObjectParams().get(1));
  }

  @Test
  void testAggregationExpressionDistinctCount() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), EQ, ConstantExpression.of(10)))
            .addSelection(
                AggregateExpression.of(DISTINCT, IdentifierExpression.of("quantity")),
                "qty_distinct")
            .addSelection(
                FunctionExpression.builder()
                    .operator(FunctionOperator.LENGTH)
                    .operand(IdentifierExpression.of("qty_distinct"))
                    .build(),
                "qty_distinct_length")
            .addAggregation(IdentifierExpression.of("item"))
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)) AS \"qty_distinct\", "
            + "ARRAY_LENGTH( ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)), 1 ) AS \"qty_distinct_length\" "
            + "FROM testCollection WHERE CAST (document->>'price' AS NUMERIC) = ? "
            + "GROUP BY document->'item'",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(10, params.getObjectParams().get(1));
  }

  @Test
  void testAggregateWithMultipleGroupingLevels() {
    Query query =
        Query.builder()
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(
                AggregateExpression.of(DISTINCT, IdentifierExpression.of("quantity")), "quantities")
            .addSelection(
                FunctionExpression.builder()
                    .operator(LENGTH)
                    .operand(IdentifierExpression.of("quantities"))
                    .build(),
                "num_quantities")
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("num_quantities"), EQ, ConstantExpression.of(1)))
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT document->'item' AS item, document->'price' AS price, "
            + "ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)) AS \"quantities\", "
            + "ARRAY_LENGTH( ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)), 1 ) AS \"num_quantities\" "
            + "FROM testCollection GROUP BY document->'item',document->'price' "
            + "HAVING ARRAY_LENGTH( ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)), 1 ) = ? "
            + "ORDER BY document->'item' DESC",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(1, params.getObjectParams().get(1));
  }

  @Test
  void testAggregationFilter() {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS item "
            + "FROM testCollection "
            + "GROUP BY document->'item' "
            + "HAVING COUNT(DISTINCT document->>'quantity' ) <= ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(10, params.getObjectParams().get(1));
  }

  @Test
  void testAggregationFilterAndWhereFilter() {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), LTE, ConstantExpression.of(7.5)))
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS item "
            + "FROM testCollection "
            + "WHERE CAST (document->>'price' AS NUMERIC) <= ? "
            + "GROUP BY document->'item' "
            + "HAVING COUNT(DISTINCT document->>'quantity' ) <= ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(7.5, params.getObjectParams().get(1));
    Assertions.assertEquals(10, params.getObjectParams().get(2));
  }

  @Test
  void testAggregationFilterAlongWithNonAliasFields() {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("price"), GT, ConstantExpression.of(5)))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS item, "
            + "document->'price' AS price "
            + "FROM testCollection "
            + "GROUP BY document->'item',document->'price' "
            + "HAVING (COUNT(DISTINCT document->>'quantity' ) <= ?) AND (CAST (document->'price' AS NUMERIC) > ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(2, params.getObjectParams().size());
    Assertions.assertEquals(10, params.getObjectParams().get(1));
    Assertions.assertEquals(5, params.getObjectParams().get(2));
  }

  @Test
  void testSimpleOrderByClause() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSort(IdentifierExpression.of("price"), ASC)
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT document->'item' AS item, document->'price' AS price "
            + "FROM testCollection "
            + "ORDER BY document->'price' ASC,document->'item' DESC",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(0, params.getObjectParams().size());
  }

  @Test
  void testOrderByClauseWithAlias() {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(1000)))
            .addSort(IdentifierExpression.of("qty_count"), DESC)
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS item "
            + "FROM testCollection "
            + "GROUP BY document->'item' "
            + "HAVING COUNT(DISTINCT document->>'quantity' ) <= ? "
            + "ORDER BY \"qty_count\" DESC,document->'item' DESC",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(1, params.getObjectParams().size());
    Assertions.assertEquals(1000, params.getObjectParams().get(1));
  }

  @Test
  void testFindWithSortingAndPagination() {
    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("quantity")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();

    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("item"),
                    IN,
                    ConstantExpression.ofStrings(List.of("Mirror", "Comb", "Shampoo", "Bottle"))))
            .build();

    Sort sort =
        Sort.builder()
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("quantity"), DESC))
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("item"), ASC))
            .build();

    Pagination pagination = Pagination.builder().offset(1).limit(3).build();

    Query query =
        Query.builder()
            .setSelection(selection)
            .setFilter(filter)
            .setSort(sort)
            .setPagination(pagination)
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT document->'item' AS item, "
            + "document->'price' AS price, "
            + "document->'quantity' AS quantity, "
            + "document->'date' AS date "
            + "FROM testCollection "
            + "WHERE document->>'item' IN (?, ?, ?, ?) "
            + "ORDER BY document->'quantity' DESC,document->'item' ASC "
            + "OFFSET ? LIMIT ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(6, params.getObjectParams().size());
    Assertions.assertEquals("Mirror", params.getObjectParams().get(1));
    Assertions.assertEquals("Comb", params.getObjectParams().get(2));
    Assertions.assertEquals("Shampoo", params.getObjectParams().get(3));
    Assertions.assertEquals("Bottle", params.getObjectParams().get(4));
    Assertions.assertEquals(1, params.getObjectParams().get(5));
    Assertions.assertEquals(3, params.getObjectParams().get(6));
  }

  @Test
  void testUnnestWithoutPreserveNullAndEmptyArrays() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), false))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), false))
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "With \n"
            + "table0 as (SELECT * from testCollection),\n"
            + "table1 as (SELECT * from table0 t0, jsonb_array_elements(document->'sales') p1(sales)),\n"
            + "table2 as (SELECT * from table1 t1, jsonb_array_elements(sales->'medium') p2(sales_dot_medium))\n"
            + "SELECT document->'item' AS item, "
            + "document->'price' AS price, "
            + "sales->'city' AS sales_dot_city, "
            + "sales_dot_medium->'type' AS sales_dot_medium_dot_type "
            + "FROM table2",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(0, params.getObjectParams().size());
  }

  @Test
  void testUnnestWithPreserveNullAndEmptyArrays() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "With \n"
            + "table0 as (SELECT * from testCollection),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(document->'sales') p1(sales) on TRUE),\n"
            + "table2 as (SELECT * from table1 t1 LEFT JOIN LATERAL jsonb_array_elements(sales->'medium') p2(sales_dot_medium) on TRUE)\n"
            + "SELECT document->'item' AS item, "
            + "document->'price' AS price, "
            + "sales->'city' AS sales_dot_city, "
            + "sales_dot_medium->'type' AS sales_dot_medium_dot_type "
            + "FROM table2",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(0, params.getObjectParams().size());
  }

  @Test
  void testUnnestWithoutPreserveNullAndEmptyArraysWithFilters() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(false)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.city"),
                            EQ,
                            ConstantExpression.of("delhi")))
                    .build())
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), false))
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "With \n"
            + "table0 as (SELECT * from testCollection "
            + "WHERE document->'quantity' IS NULL OR CAST (document->>'quantity' AS NUMERIC) != ?),\n"
            + "table1 as (SELECT * from table0 t0, jsonb_array_elements(document->'sales') p1(sales)),\n"
            + "table2 as (SELECT * from table1 t1, jsonb_array_elements(sales->'medium') p2(sales_dot_medium))\n"
            + "SELECT document->'item' AS item, "
            + "sales->'city' AS sales_dot_city, "
            + "sales_dot_medium->'type' AS sales_dot_medium_dot_type "
            + "FROM table2 WHERE sales->>'city' = ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(2, params.getObjectParams().size());
    Assertions.assertEquals(10, params.getObjectParams().get(1));
    Assertions.assertEquals("delhi", params.getObjectParams().get(2));
  }

  @Test
  void testQueryQ1AggregationFilterWithStringAlongWithNonAliasFields() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"), EQ, ConstantExpression.of("Soap")))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS item, document->'price' AS price "
            + "FROM testCollection GROUP BY document->'item',document->'price' "
            + "HAVING (COUNT(DISTINCT document->>'quantity' ) <= ?) AND (CAST (document->'item' AS TEXT) = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(2, params.getObjectParams().size());
    Assertions.assertEquals(10, params.getObjectParams().get(1));
    Assertions.assertEquals("\"Soap\"", params.getObjectParams().get(2));
  }

  @Test
  void testQueryQ1AggregationFilterWithStringInFilterAlongWithNonAliasFields() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"),
                            IN,
                            ConstantExpression.ofStrings(
                                List.of("Mirror", "Comb", "Shampoo", "Bottle"))))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();

    Assertions.assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS item, document->'price' AS price "
            + "FROM testCollection GROUP BY document->'item',document->'price' "
            + "HAVING (COUNT(DISTINCT document->>'quantity' ) <= ?) AND (CAST (document->'item' AS TEXT) IN (?, ?, ?, ?))",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(5, params.getObjectParams().size());
    Assertions.assertEquals(10, params.getObjectParams().get(1));
    Assertions.assertEquals("\"Mirror\"", params.getObjectParams().get(2));
    Assertions.assertEquals("\"Comb\"", params.getObjectParams().get(3));
    Assertions.assertEquals("\"Shampoo\"", params.getObjectParams().get(4));
    Assertions.assertEquals("\"Bottle\"", params.getObjectParams().get(5));
  }
}
