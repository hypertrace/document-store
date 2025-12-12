package org.hypertrace.core.documentstore.postgres.query.v1;

import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.and;
import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.or;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_ARRAY;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.PostgresTableIdentifier;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FlatPostgresFieldTransformer;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.PostgresQueryTransformer;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class PostgresQueryParserTest {

  private static final PostgresTableIdentifier TEST_TABLE =
      PostgresTableIdentifier.parse("testCollection");
  private static final String TENANT_ID = "tenant-id";

  @Test
  void testParseSimpleFilter() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .build();
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * "
            + "FROM \"testCollection\" "
            + "WHERE CAST (document->>'quantity' AS NUMERIC) != ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(10, params.getObjectParams().get(1));
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
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * "
            + "FROM \"testCollection\" "
            + "WHERE (CAST (document->>'quantity' AS NUMERIC) > ?) "
            + "AND (document->'props'->'seller'->'address'->>'city' = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(5, params.getObjectParams().get(1));
    assertEquals("Kolkata", params.getObjectParams().get(2));
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
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * "
            + "FROM \"testCollection\" "
            + "WHERE (CAST (document->>'quantity' AS NUMERIC) >= ?) "
            + "AND (CAST (document->>'quantity' AS NUMERIC) <= ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(5, params.getObjectParams().get(1));
    assertEquals(10, params.getObjectParams().get(2));
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
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * "
            + "FROM \"testCollection\" "
            + "WHERE (CAST (document->>'quantity' AS NUMERIC) >= ?) "
            + "OR (CAST (document->>'quantity' AS NUMERIC) <= ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(5, params.getObjectParams().get(1));
    assertEquals(10, params.getObjectParams().get(2));
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * "
            + "FROM \"testCollection\" "
            + "WHERE (CAST (document->>'price' AS NUMERIC) >= ?) "
            + "AND ((CAST (document->>'quantity' AS NUMERIC) >= ?) "
            + "OR (CAST (document->>'quantity' AS NUMERIC) <= ?))",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(5, params.getObjectParams().get(1));
    assertEquals(5, params.getObjectParams().get(2));
    assertEquals(10, params.getObjectParams().get(3));
  }

  @Test
  void testBasicSelectionExpression() {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .build();
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT document->'item' AS \"item\", document->'price' AS \"price\" "
            + "FROM \"testCollection\"",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(0, params.getObjectParams().size());
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
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT document->'item' AS \"item\", "
            + "CAST (document->>'price' AS NUMERIC) * CAST (document->>'quantity' AS NUMERIC) AS \"total\" "
            + "FROM \"testCollection\"",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(0, params.getObjectParams().size());
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
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT document->'item' AS \"item\", "
            + "document->'props'->'brand' AS \"props_dot_brand\", "
            + "document->'props'->'seller'->'name' AS \"props_dot_seller_dot_name\", "
            + "CAST (document->>'price' AS NUMERIC) * CAST (document->>'quantity' AS NUMERIC) AS \"total\" "
            + "FROM \"testCollection\"",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(0, params.getObjectParams().size());
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
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT document->'item' AS \"item\", "
            + "document->'props'->'brand' AS \"props_band\", "
            + "document->'props'->'seller'->'name' AS \"props_seller_name\", "
            + "CAST (document->>'price' AS NUMERIC) * CAST (document->>'quantity' AS NUMERIC) AS \"total\" "
            + "FROM \"testCollection\"",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(0, params.getObjectParams().size());
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT document->'item' AS \"item\", "
            + "AVG( CAST (document->>'quantity' AS NUMERIC) ) AS \"qty_avg\", "
            + "COUNT( document->>'quantity' ) AS \"qty_count\", "
            + "COUNT(DISTINCT document->>'quantity' ) AS \"qty_distinct_count\", "
            + "SUM( CAST (document->>'quantity' AS NUMERIC) ) AS \"qty_sum\", "
            + "MIN( CAST (document->>'quantity' AS NUMERIC) ) AS \"qty_min\", "
            + "MAX( CAST (document->>'quantity' AS NUMERIC) ) AS \"qty_max\" "
            + "FROM \"testCollection\" WHERE CAST (document->>'price' AS NUMERIC) = ? "
            + "GROUP BY document->'item'",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals(10, params.getObjectParams().get(1));
  }

  @Test
  void testAggregationExpressionDistinctCount() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), EQ, ConstantExpression.of(10)))
            .addSelection(
                AggregateExpression.of(DISTINCT_ARRAY, IdentifierExpression.of("quantity")),
                "qty_distinct")
            .addSelection(
                FunctionExpression.builder()
                    .operator(FunctionOperator.LENGTH)
                    .operand(IdentifierExpression.of("qty_distinct"))
                    .build(),
                "qty_distinct_length")
            .addAggregation(IdentifierExpression.of("item"))
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)) AS \"qty_distinct\", "
            + "ARRAY_LENGTH( ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)), 1 ) AS \"qty_distinct_length\" "
            + "FROM \"testCollection\" "
            + "WHERE CAST (document->>'price' AS NUMERIC) = ? "
            + "GROUP BY document->'item'",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(10, params.getObjectParams().get(1));
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
                AggregateExpression.of(DISTINCT_ARRAY, IdentifierExpression.of("quantity")),
                "quantities")
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT document->'item' AS \"item\", document->'price' AS \"price\", "
            + "ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)) AS \"quantities\", "
            + "ARRAY_LENGTH( ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)), 1 ) AS \"num_quantities\" "
            + "FROM \"testCollection\" "
            + "GROUP BY document->'item',document->'price' "
            + "HAVING ARRAY_LENGTH( ARRAY_AGG(DISTINCT CAST (document->>'quantity' AS NUMERIC)), 1 ) = ? "
            + "ORDER BY document->'item' DESC NULLS LAST",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().get(1));
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS \"item\" "
            + "FROM \"testCollection\" "
            + "GROUP BY document->'item' "
            + "HAVING COUNT(DISTINCT document->>'quantity' ) <= ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(10, params.getObjectParams().get(1));
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS \"item\" "
            + "FROM \"testCollection\" "
            + "WHERE CAST (document->>'price' AS NUMERIC) <= ? "
            + "GROUP BY document->'item' "
            + "HAVING COUNT(DISTINCT document->>'quantity' ) <= ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(7.5, params.getObjectParams().get(1));
    assertEquals(10, params.getObjectParams().get(2));
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS \"item\", "
            + "document->'price' AS \"price\" "
            + "FROM \"testCollection\" "
            + "GROUP BY document->'item',document->'price' "
            + "HAVING (COUNT(DISTINCT document->>'quantity' ) <= ?) AND (CAST (document->'price' AS NUMERIC) > ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(2, params.getObjectParams().size());
    assertEquals(10, params.getObjectParams().get(1));
    assertEquals(5, params.getObjectParams().get(2));
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT document->'item' AS \"item\", document->'price' AS \"price\" "
            + "FROM \"testCollection\" "
            + "ORDER BY document->'price' ASC NULLS FIRST,document->'item' DESC NULLS LAST",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(0, params.getObjectParams().size());
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS \"item\" "
            + "FROM \"testCollection\" "
            + "GROUP BY document->'item' "
            + "HAVING COUNT(DISTINCT document->>'quantity' ) <= ? "
            + "ORDER BY \"qty_count\" DESC NULLS LAST,document->'item' DESC NULLS LAST",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals(1000, params.getObjectParams().get(1));
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

    // json field
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT document->'item' AS \"item\", "
            + "document->'price' AS \"price\", "
            + "document->'quantity' AS \"quantity\", "
            + "document->'date' AS \"date\" "
            + "FROM \"testCollection\" "
            + "WHERE (((jsonb_typeof(to_jsonb(document->'item')) = 'array' AND to_jsonb(document->'item') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'item') @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(document->'item')) = 'array' AND to_jsonb(document->'item') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'item') @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(document->'item')) = 'array' AND to_jsonb(document->'item') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'item') @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(document->'item')) = 'array' AND to_jsonb(document->'item') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'item') @> jsonb_build_array(?)))) "
            + "ORDER BY document->'quantity' DESC NULLS LAST,document->'item' ASC NULLS FIRST "
            + "OFFSET ? LIMIT ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(10, params.getObjectParams().size());
    assertEquals("Mirror", params.getObjectParams().get(1));
    assertEquals("Comb", params.getObjectParams().get(3));
    assertEquals("Shampoo", params.getObjectParams().get(5));
    assertEquals("Bottle", params.getObjectParams().get(7));
    assertEquals(1, params.getObjectParams().get(9));
    assertEquals(3, params.getObjectParams().get(10));

    // non-json field
    postgresQueryParser =
        new PostgresQueryParser(
            TEST_TABLE,
            PostgresQueryTransformer.transform(query),
            new FlatPostgresFieldTransformer());
    assertEquals(
        "SELECT \"item\" AS \"item\", "
            + "\"price\" AS \"price\", "
            + "\"quantity\" AS \"quantity\", "
            + "\"date\" AS \"date\" "
            + "FROM \"testCollection\" "
            + "WHERE \"item\" IN (?, ?, ?, ?) "
            + "ORDER BY \"quantity\" DESC NULLS LAST,\"item\" ASC NULLS FIRST "
            + "OFFSET ? LIMIT ?",
        postgresQueryParser.parse());
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\"),\n"
            + "table1 as (SELECT * from table0 t0, jsonb_array_elements(document->'sales') p1(\"sales\")),\n"
            + "table2 as (SELECT * from table1 t1, jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\"))\n"
            + "SELECT document->'item' AS \"item\", "
            + "document->'price' AS \"price\", "
            + "sales->'city' AS \"sales_dot_city\", "
            + "sales_dot_medium->'type' AS \"sales_dot_medium_dot_type\" "
            + "FROM table2",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(0, params.getObjectParams().size());
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\"),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(document->'sales') p1(\"sales\") on TRUE),\n"
            + "table2 as (SELECT * from table1 t1 LEFT JOIN LATERAL jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\") on TRUE)\n"
            + "SELECT document->'item' AS \"item\", "
            + "document->'price' AS \"price\", "
            + "sales->'city' AS \"sales_dot_city\", "
            + "sales_dot_medium->'type' AS \"sales_dot_medium_dot_type\" "
            + "FROM table2",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(0, params.getObjectParams().size());
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\" "
            + "WHERE CAST (document->>'quantity' AS NUMERIC) != ?),\n"
            + "table1 as (SELECT * from table0 t0, jsonb_array_elements(document->'sales') p1(\"sales\")),\n"
            + "table2 as (SELECT * from table1 t1, jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\"))\n"
            + "SELECT document->'item' AS \"item\", "
            + "sales->'city' AS \"sales_dot_city\", "
            + "sales_dot_medium->'type' AS \"sales_dot_medium_dot_type\" "
            + "FROM table2 WHERE sales->>'city' = ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(2, params.getObjectParams().size());
    assertEquals(10, params.getObjectParams().get(1));
    assertEquals("delhi", params.getObjectParams().get(2));
  }

  @Test
  void testUnnestWithRegularFilterAtSecondLevelArray() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium.type"),
                            EQ,
                            ConstantExpression.of("retail")))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\" WHERE CAST (document->>'quantity' AS NUMERIC) > ?),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(document->'sales') p1(\"sales\") on TRUE),\n"
            + "table2 as (SELECT * from table1 t1 LEFT JOIN LATERAL jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\") on TRUE)\n"
            + "SELECT document->'item' AS \"item\", document->'price' AS \"price\", sales->'city' AS \"sales_dot_city\", sales_dot_medium->'type' AS \"sales_dot_medium_dot_type\" FROM table2 WHERE sales_dot_medium->>'type' = ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(2, params.getObjectParams().size());
  }

  @Test
  void testUnnestWithRegularORFilterAtSecondLevelArray() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .setFilter(
                LogicalExpression.builder()
                    .operator(OR)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium.type"),
                            EQ,
                            ConstantExpression.of("retail")))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\"),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(document->'sales') p1(\"sales\") on TRUE),\n"
            + "table2 as (SELECT * from table1 t1 LEFT JOIN LATERAL jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\") on TRUE)\n"
            + "SELECT document->'item' AS \"item\", document->'price' AS \"price\", sales->'city' AS \"sales_dot_city\", sales_dot_medium->'type' AS \"sales_dot_medium_dot_type\" FROM table2 WHERE (CAST (document->>'quantity' AS NUMERIC) > ?) OR (sales_dot_medium->>'type' = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(2, params.getObjectParams().size());
  }

  @Test
  void testUnnestWithRegularAndORFilterAtSecondLevelArray() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("price"), GT, ConstantExpression.of(10)))
                    .operand(
                        LogicalExpression.builder()
                            .operator(OR)
                            .operand(
                                RelationalExpression.of(
                                    IdentifierExpression.of("quantity"),
                                    GT,
                                    ConstantExpression.of(5)))
                            .operand(
                                RelationalExpression.of(
                                    IdentifierExpression.of("sales.medium.type"),
                                    EQ,
                                    ConstantExpression.of("retail")))
                            .build())
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\" WHERE CAST (document->>'price' AS NUMERIC) > ?),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(document->'sales') p1(\"sales\") on TRUE),\n"
            + "table2 as (SELECT * from table1 t1 LEFT JOIN LATERAL jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\") on TRUE)\n"
            + "SELECT document->'item' AS \"item\", document->'price' AS \"price\", sales->'city' AS \"sales_dot_city\", sales_dot_medium->'type' AS \"sales_dot_medium_dot_type\" FROM table2 WHERE (CAST (document->>'quantity' AS NUMERIC) > ?) OR (sales_dot_medium->>'type' = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(3, params.getObjectParams().size());
  }

  @Test
  void testUnnestWithRegularAndUnnestFilterAtSecondLevelArray() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("quantity"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales.medium"))
                    .preserveNullAndEmptyArrays(true)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium.type"),
                            EQ,
                            ConstantExpression.of("retail")))
                    .build())
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium.type"),
                            EQ,
                            ConstantExpression.of("retail")))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\" WHERE CAST (document->>'quantity' AS NUMERIC) > ?),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(document->'sales') p1(\"sales\") on TRUE),\n"
            + "table2 as (SELECT * from table1 t1 LEFT JOIN LATERAL jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\") on TRUE)\n"
            + "SELECT document->'item' AS \"item\", document->'quantity' AS \"quantity\", sales->'city' AS \"sales_dot_city\", sales_dot_medium->'type' AS \"sales_dot_medium_dot_type\" FROM table2 WHERE (sales_dot_medium->>'type' = ?) AND (sales_dot_medium->>'type' = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(3, params.getObjectParams().size());
  }

  @Test
  void testUnnestWithRegularAndDifferentUnnestFilterAtSecondLevelArray() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("quantity"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales.medium"))
                    .preserveNullAndEmptyArrays(true)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium.channel"),
                            EQ,
                            ConstantExpression.of("online")))
                    .build())
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium.type"),
                            EQ,
                            ConstantExpression.of("retail")))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\" WHERE CAST (document->>'quantity' AS NUMERIC) > ?),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(document->'sales') p1(\"sales\") on TRUE),\n"
            + "table2 as (SELECT * from table1 t1 LEFT JOIN LATERAL jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\") on TRUE)\n"
            + "SELECT document->'item' AS \"item\", document->'quantity' AS \"quantity\", sales->'city' AS \"sales_dot_city\", sales_dot_medium->'type' AS \"sales_dot_medium_dot_type\" FROM table2 WHERE (sales_dot_medium->>'type' = ?) AND (sales_dot_medium->>'channel' = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(3, params.getObjectParams().size());
  }

  @Test
  void testUnnestWithRegularAndDifferentUnnestFilterAtFirstLevelArray() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("quantity"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(true)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.city"),
                            EQ,
                            ConstantExpression.of("mumbai")))
                    .build())
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.channel"),
                            EQ,
                            ConstantExpression.of("oneline")))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\" WHERE CAST (document->>'quantity' AS NUMERIC) > ?),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(document->'sales') p1(\"sales\") on TRUE),\n"
            + "table2 as (SELECT * from table1 t1 LEFT JOIN LATERAL jsonb_array_elements(sales->'medium') p2(\"sales_dot_medium\") on TRUE)\n"
            + "SELECT document->'item' AS \"item\", document->'quantity' AS \"quantity\", sales->'city' AS \"sales_dot_city\" FROM table2 WHERE (sales->>'channel' = ?) AND (sales->>'city' = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(3, params.getObjectParams().size());
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS \"item\", document->'price' AS \"price\" "
            + "FROM \"testCollection\" GROUP BY document->'item',document->'price' "
            + "HAVING (COUNT(DISTINCT document->>'quantity' ) <= ?) AND (CAST (document->'item' AS TEXT) = ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(2, params.getObjectParams().size());
    assertEquals(10, params.getObjectParams().get(1));
    assertEquals("\"Soap\"", params.getObjectParams().get(2));
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

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS \"item\", document->'price' AS \"price\" "
            + "FROM \"testCollection\" GROUP BY document->'item',document->'price' "
            + "HAVING (COUNT(DISTINCT document->>'quantity' ) <= ?) AND ((((jsonb_typeof(to_jsonb(CAST (document->'item' AS TEXT))) = 'array' AND to_jsonb(CAST (document->'item' AS TEXT)) @> jsonb_build_array(?)) OR (jsonb_build_array(CAST (document->'item' AS TEXT)) @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(CAST (document->'item' AS TEXT))) = 'array' AND to_jsonb(CAST (document->'item' AS TEXT)) @> jsonb_build_array(?)) OR (jsonb_build_array(CAST (document->'item' AS TEXT)) @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(CAST (document->'item' AS TEXT))) = 'array' AND to_jsonb(CAST (document->'item' AS TEXT)) @> jsonb_build_array(?)) OR (jsonb_build_array(CAST (document->'item' AS TEXT)) @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(CAST (document->'item' AS TEXT))) = 'array' AND to_jsonb(CAST (document->'item' AS TEXT)) @> jsonb_build_array(?)) OR (jsonb_build_array(CAST (document->'item' AS TEXT)) @> jsonb_build_array(?)))))",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(9, params.getObjectParams().size());
    assertEquals(10, params.getObjectParams().get(1));
    assertEquals("\"Mirror\"", params.getObjectParams().get(2));
    assertEquals("\"Comb\"", params.getObjectParams().get(4));
    assertEquals("\"Shampoo\"", params.getObjectParams().get(6));
    assertEquals("\"Bottle\"", params.getObjectParams().get(8));
  }

  @Test
  void testQueryQ1DistinctCountAggregationWithOnlyFilter() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("price"), LTE, ConstantExpression.of(10)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"),
                            IN,
                            ConstantExpression.ofStrings(
                                List.of("Mirror", "Comb", "Shampoo", "Bottle"))))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\" "
            + "FROM \"testCollection\" "
            + "WHERE (CAST (document->>'price' AS NUMERIC) <= ?) AND ((((jsonb_typeof(to_jsonb(document->'item')) = 'array' AND to_jsonb(document->'item') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'item') @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(document->'item')) = 'array' AND to_jsonb(document->'item') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'item') @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(document->'item')) = 'array' AND to_jsonb(document->'item') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'item') @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(document->'item')) = 'array' AND to_jsonb(document->'item') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'item') @> jsonb_build_array(?)))))",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(9, params.getObjectParams().size());
    assertEquals(10, params.getObjectParams().get(1));
    assertEquals("Mirror", params.getObjectParams().get(2));
    assertEquals("Comb", params.getObjectParams().get(4));
    assertEquals("Shampoo", params.getObjectParams().get(6));
    assertEquals("Bottle", params.getObjectParams().get(8));
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

    final PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    final String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT document->'item' AS \"item\" "
            + "FROM \"testCollection\" "
            + "WHERE CAST (document->>'quantity' AS NUMERIC) * CAST (document->>'price' AS NUMERIC) > ? "
            + "ORDER BY document->'item' DESC NULLS LAST",
        sql);

    final Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals(50, params.getObjectParams().get(1));
  }

  @Test
  void testQueryQ1DistinctCountAggregationWithMatchingSelectionAndGroupBy() {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), LTE, ConstantExpression.of(10)))
            .addAggregation(IdentifierExpression.of("item"))
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT COUNT(DISTINCT document->>'quantity' ) AS \"qty_count\", "
            + "document->'item' AS \"item\" "
            + "FROM \"testCollection\" WHERE CAST (document->>'price' AS NUMERIC) <= ? "
            + "GROUP BY document->'item'",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals(10, params.getObjectParams().get(1));
  }

  @Test
  public void testFindWithSingleKey() {
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(KeyExpression.of(new SingleValueKey(TENANT_ID, "7")))
            .build();

    final PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(
            TEST_TABLE,
            PostgresQueryTransformer.transform(Query.builder().setFilter(filter).build()));
    final String sql = postgresQueryParser.parse();

    assertEquals("SELECT * FROM \"testCollection\" WHERE id = ?", sql);

    final Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals(TENANT_ID + ":7", params.getObjectParams().get(1));
  }

  @Test
  public void testFindWithMultipleKeys() {
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                or(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "30"))))
            .build();

    final PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(
            TEST_TABLE,
            PostgresQueryTransformer.transform(Query.builder().setFilter(filter).build()));
    final String sql = postgresQueryParser.parse();

    assertEquals("SELECT * FROM \"testCollection\" WHERE (id = ?) OR (id = ?)", sql);

    final Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(2, params.getObjectParams().size());
    assertEquals(TENANT_ID + ":7", params.getObjectParams().get(1));
    assertEquals(TENANT_ID + ":30", params.getObjectParams().get(2));
  }

  @Test
  public void testFindWithKeyAndRelationalFilter() {
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                and(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    RelationalExpression.of(
                        IdentifierExpression.of("item"), NEQ, ConstantExpression.of("Comb"))))
            .build();
    final PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(
            TEST_TABLE,
            PostgresQueryTransformer.transform(Query.builder().setFilter(filter).build()));
    final String sql = postgresQueryParser.parse();

    assertEquals(
        "SELECT * FROM \"testCollection\" WHERE (id = ?) AND (document->>'item' != ?)", sql);

    final Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(2, params.getObjectParams().size());
    assertEquals(TENANT_ID + ":7", params.getObjectParams().get(1));
    assertEquals("Comb", params.getObjectParams().get(2));
  }

  @Test
  void testContainsFilter() throws IOException {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("sales"),
                    CONTAINS,
                    ConstantExpression.of(new JSONDocument("\"a\""))))
            .build();
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();
    assertEquals("SELECT * FROM \"testCollection\" WHERE document->'sales' @> ?::jsonb", sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals("[\"a\"]", params.getObjectParams().get(1));
  }

  @Test
  void testContainsAndUnnestFilters() throws IOException {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("sales.medium"))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(false)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium"),
                            CONTAINS,
                            ConstantExpression.of(
                                new JSONDocument("{\"type\": \"retail\",\"volume\": 500}"))))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\"),\n"
            + "table1 as (SELECT * from table0 t0, jsonb_array_elements(document->'sales') p1(\"sales\"))\n"
            + "SELECT document->'item' AS \"item\", sales->'medium' AS \"sales_dot_medium\" FROM table1 WHERE sales->'medium' @> ?::jsonb",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals("[{\"type\":\"retail\",\"volume\":500}]", params.getObjectParams().get(1));
  }

  @Test
  void testNotContainsAndUnnestFilters() throws IOException {
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("sales.medium"))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(false)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium"),
                            NOT_CONTAINS,
                            ConstantExpression.of(
                                new JSONDocument("{\"type\": \"retail\",\"volume\": 500}"))))
                    .build())
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));
    String sql = postgresQueryParser.parse();

    assertEquals(
        "With \n"
            + "table0 as (SELECT * from \"testCollection\"),\n"
            + "table1 as (SELECT * from table0 t0, jsonb_array_elements(document->'sales') p1(\"sales\"))\n"
            + "SELECT document->'item' AS \"item\", sales->'medium' AS \"sales_dot_medium\" FROM table1 WHERE sales->'medium' IS NULL OR NOT sales->'medium' @> ?::jsonb",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals("[{\"type\":\"retail\",\"volume\":500}]", params.getObjectParams().get(1));
  }

  @Test
  void testCollectionInOtherSchema() {
    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(
            PostgresTableIdentifier.parse("test_schema.test_table.with_a_dot"),
            PostgresQueryTransformer.transform(Query.builder().build()));

    assertEquals(
        "SELECT * FROM test_schema.\"test_table.with_a_dot\"", postgresQueryParser.parse());
  }

  @Test
  void testNotContainsWithFlatCollectionNonJsonField() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    ArrayIdentifierExpression.ofStrings("tags"),
                    NOT_CONTAINS,
                    ConstantExpression.of("premium")))
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(
            TEST_TABLE,
            PostgresQueryTransformer.transform(query),
            new FlatPostgresFieldTransformer());

    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * FROM \"testCollection\" WHERE \"tags\" IS NULL OR NOT (\"tags\" @> ARRAY[?]::text[])",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals("premium", params.getObjectParams().get(1));
  }

  @Test
  void testNotContainsWithNestedCollectionJsonField() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("attributes"),
                    NOT_CONTAINS,
                    ConstantExpression.of("value1")))
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));

    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * FROM \"testCollection\" WHERE document->'attributes' IS NULL OR NOT document->'attributes' @> ?::jsonb",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals("[\"value1\"]", params.getObjectParams().get(1));
  }

  @Test
  void testNotInWithFlatCollectionNonJsonField() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("category"),
                    NOT_IN,
                    ConstantExpression.ofStrings(List.of("electronics", "clothing"))))
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(
            TEST_TABLE,
            PostgresQueryTransformer.transform(query),
            new FlatPostgresFieldTransformer());

    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * FROM \"testCollection\" WHERE \"category\" IS NULL OR NOT (\"category\" IN (?, ?))",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(2, params.getObjectParams().size());
  }

  @Test
  void testNotInWithNestedCollectionJsonField() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("status"),
                    NOT_IN,
                    ConstantExpression.ofStrings(List.of("active", "pending"))))
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(TEST_TABLE, PostgresQueryTransformer.transform(query));

    String sql = postgresQueryParser.parse();
    assertEquals(
        "SELECT * FROM \"testCollection\" WHERE document->'status' IS NULL OR NOT ((((jsonb_typeof(to_jsonb(document->'status')) = 'array' AND to_jsonb(document->'status') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'status') @> jsonb_build_array(?))) OR ((jsonb_typeof(to_jsonb(document->'status')) = 'array' AND to_jsonb(document->'status') @> jsonb_build_array(?)) OR (jsonb_build_array(document->'status') @> jsonb_build_array(?)))))",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(4, params.getObjectParams().size());
  }

  @Test
  void testContainsWithFlatCollectionNonJsonField() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    ArrayIdentifierExpression.ofStrings("keywords"),
                    CONTAINS,
                    ConstantExpression.of("java")))
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(
            TEST_TABLE,
            PostgresQueryTransformer.transform(query),
            new FlatPostgresFieldTransformer());

    String sql = postgresQueryParser.parse();
    assertEquals("SELECT * FROM \"testCollection\" WHERE \"keywords\" @> ARRAY[?]::text[]", sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals(1, params.getObjectParams().size());
    assertEquals("java", params.getObjectParams().get(1));
  }

  @Nested
  class FlatCollectionExistsNotExistsParserTest {

    @Test
    void testExistsOnTopLevelScalarField() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("status"), EXISTS, ConstantExpression.of("null")))
              .build();

      PostgresQueryParser postgresQueryParser =
          new PostgresQueryParser(
              TEST_TABLE,
              PostgresQueryTransformer.transform(query),
              new FlatPostgresFieldTransformer());

      String sql = postgresQueryParser.parse();
      assertEquals("SELECT * FROM \"testCollection\" WHERE \"status\" IS NOT NULL", sql);

      Params params = postgresQueryParser.getParamsBuilder().build();
      assertEquals(0, params.getObjectParams().size());
    }

    @Test
    void testExistsOnTopLevelArrayField() {
      // Query on IS_NOT_EMPTY, returns arrays with length > 0
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.ofStrings("tags"),
                      EXISTS,
                      ConstantExpression.of("null")))
              .build();

      PostgresQueryParser postgresQueryParser =
          new PostgresQueryParser(
              TEST_TABLE,
              PostgresQueryTransformer.transform(query),
              new FlatPostgresFieldTransformer());

      String sql = postgresQueryParser.parse();
      assertEquals("SELECT * FROM \"testCollection\" WHERE (cardinality(\"tags\") > 0)", sql);

      Params params = postgresQueryParser.getParamsBuilder().build();
      assertEquals(0, params.getObjectParams().size());
    }

    @Test
    void testExistsOnJsonbScalarField() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("customAttribute", JsonFieldType.STRING, "brand"),
                      EXISTS,
                      ConstantExpression.of("null")))
              .build();

      PostgresQueryParser postgresQueryParser =
          new PostgresQueryParser(
              TEST_TABLE,
              PostgresQueryTransformer.transform(query),
              new FlatPostgresFieldTransformer());

      String sql = postgresQueryParser.parse();
      assertEquals(
          "SELECT * FROM \"testCollection\" WHERE \"customAttribute\"->'brand' IS NOT NULL", sql);

      Params params = postgresQueryParser.getParamsBuilder().build();
      assertEquals(0, params.getObjectParams().size());
    }

    @Test
    void testExistsOnJsonbArrayField() {
      // Query on IS_NOT_EMPTY, returns arrays with length > 0
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors"),
                      EXISTS,
                      ConstantExpression.of("null")))
              .build();

      PostgresQueryParser postgresQueryParser =
          new PostgresQueryParser(
              TEST_TABLE,
              PostgresQueryTransformer.transform(query),
              new FlatPostgresFieldTransformer());

      String sql = postgresQueryParser.parse();
      assertEquals(
          "SELECT * FROM \"testCollection\" WHERE (\"props\" @> '{\"colors\": []}' AND jsonb_array_length(\"props\"->'colors') > 0)",
          sql);

      Params params = postgresQueryParser.getParamsBuilder().build();
      assertEquals(0, params.getObjectParams().size());
    }

    @Test
    void testNotExistsOnScalarField() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("status"), NOT_EXISTS, ConstantExpression.of("null")))
              .build();

      PostgresQueryParser postgresQueryParser =
          new PostgresQueryParser(
              TEST_TABLE,
              PostgresQueryTransformer.transform(query),
              new FlatPostgresFieldTransformer());

      String sql = postgresQueryParser.parse();
      assertEquals("SELECT * FROM \"testCollection\" WHERE \"status\" IS NULL", sql);

      Params params = postgresQueryParser.getParamsBuilder().build();
      assertEquals(0, params.getObjectParams().size());
    }

    @Test
    void testNotExistsOnArrayField() {
      // Query on IS_NOT_EMPTY, returns arrays with length > 0
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.ofStrings("tags"),
                      NOT_EXISTS,
                      ConstantExpression.of("null")))
              .build();

      PostgresQueryParser postgresQueryParser =
          new PostgresQueryParser(
              TEST_TABLE,
              PostgresQueryTransformer.transform(query),
              new FlatPostgresFieldTransformer());

      String sql = postgresQueryParser.parse();
      assertEquals(
          "SELECT * FROM \"testCollection\" WHERE COALESCE(cardinality(\"tags\"), 0) = 0", sql);

      Params params = postgresQueryParser.getParamsBuilder().build();
      assertEquals(0, params.getObjectParams().size());
    }

    @Test
    void testNotExistsOnJsonbScalarField() {
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("customAttribute", JsonFieldType.STRING, "brand"),
                      NOT_EXISTS,
                      ConstantExpression.of("null")))
              .build();

      PostgresQueryParser postgresQueryParser =
          new PostgresQueryParser(
              TEST_TABLE,
              PostgresQueryTransformer.transform(query),
              new FlatPostgresFieldTransformer());

      String sql = postgresQueryParser.parse();
      assertEquals(
          "SELECT * FROM \"testCollection\" WHERE \"customAttribute\"->'brand' IS NULL", sql);

      Params params = postgresQueryParser.getParamsBuilder().build();
      assertEquals(0, params.getObjectParams().size());
    }

    @Test
    void testNotExistsOnJsonbArrayField() {
      // this is a IS_EMPTY query in a nested jsonb array. Returns arrays that are either NULL or
      // empty arrays
      Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors"),
                      NOT_EXISTS,
                      ConstantExpression.of("null")))
              .build();

      PostgresQueryParser postgresQueryParser =
          new PostgresQueryParser(
              TEST_TABLE,
              PostgresQueryTransformer.transform(query),
              new FlatPostgresFieldTransformer());

      String sql = postgresQueryParser.parse();
      assertEquals(
          "SELECT * FROM \"testCollection\" WHERE COALESCE(jsonb_array_length(\"props\"->'colors'), 0) = 0",
          sql);

      Params params = postgresQueryParser.getParamsBuilder().build();
      assertEquals(0, params.getObjectParams().size());
    }
  }

  @Test
  void testFlatCollectionWithHyphenatedJsonbArrayFieldInUnnest() {
    // This test reproduces the syntax error with field names containing hyphens
    // When a JSONB array field with hyphens (e.g., "dev-ops-owner") is unnested,
    // the alias becomes "customAttribute_dot_dev-ops-owner" which needs quotes in LATERAL join
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("id"))
            .addSelection(
                JsonIdentifierExpression.of(
                    "customAttribute", JsonFieldType.STRING_ARRAY, "dev-ops-owner"))
            .addFromClause(
                UnnestExpression.of(
                    JsonIdentifierExpression.of(
                        "customAttribute", JsonFieldType.STRING_ARRAY, "dev-ops-owner"),
                    true))
            .setFilter(
                RelationalExpression.of(
                    JsonIdentifierExpression.of(
                        "customAttribute", JsonFieldType.STRING_ARRAY, "dev-ops-owner"),
                    EQ,
                    ConstantExpression.of("team-alpha")))
            .build();

    PostgresQueryParser postgresQueryParser =
        new PostgresQueryParser(
            TEST_TABLE,
            PostgresQueryTransformer.transform(query),
            new FlatPostgresFieldTransformer());

    String sql = postgresQueryParser.parse();

    String expectedSql =
        "With \n"
            + "table0 as (SELECT * from \"testCollection\"),\n"
            + "table1 as (SELECT * from table0 t0 LEFT JOIN LATERAL jsonb_array_elements(\"customAttribute\"->'dev-ops-owner') p1(\"customAttribute_dot_dev-ops-owner\") on TRUE)\n"
            + "SELECT \"id\" AS \"id\", \"customAttribute_dot_dev-ops-owner\" AS \"customAttribute_dot_dev-ops-owner\" "
            + "FROM table1 WHERE \"customAttribute_dot_dev-ops-owner\" = ?";

    assertEquals(expectedSql, sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    assertEquals("team-alpha", params.getObjectParams().get(1));
  }
}
