package org.hypertrace.core.documentstore.postgres.query.v1;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.query.Query;
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
            + "WHERE document->'quantity' IS NULL OR CAST (document->'quantity' AS NUMERIC) != ?",
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
                            IdentifierExpression.of("props.address.city"),
                            EQ,
                            ConstantExpression.of("Kolkata")))
                    .build())
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    String sql = postgresQueryParser.parse();
    Assertions.assertEquals(
        "SELECT * FROM testCollection "
            + "WHERE (CAST (document->'quantity' AS NUMERIC) > ?) "
            + "AND (CAST (document->'props'->'address'->'city' AS STRING) = ?)",
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
        "SELECT * FROM testCollection WHERE (CAST (document->'quantity' AS NUMERIC) >= ?) "
            + "AND (CAST (document->'quantity' AS NUMERIC) <= ?)",
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
        "SELECT * FROM testCollection WHERE (CAST (document->'quantity' AS NUMERIC) >= ?) "
            + "OR (CAST (document->'quantity' AS NUMERIC) <= ?)",
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
        "SELECT * FROM testCollection WHERE (CAST (document->'price' AS NUMERIC) >= ?) "
            + "AND ((CAST (document->'quantity' AS NUMERIC) >= ?) "
            + "OR (CAST (document->'quantity' AS NUMERIC) <= ?))",
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
        "SELECT document->'item' AS item, CAST (document->>'price' AS NUMERIC) "
            + "* CAST (document->>'quantity' AS NUMERIC) AS total FROM testCollection",
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
        "SELECT document->'item' AS item, document->'props'->'brand' AS props_dot_brand, "
            + "document->'props'->'seller'->'name' AS props_dot_seller_dot_name, "
            + "CAST (document->>'price' AS NUMERIC) * CAST (document->>'quantity' AS NUMERIC) AS total "
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
        "SELECT document->'item' AS item, document->'props'->'brand' AS props_band, "
            + "document->'props'->'seller'->'name' AS props_seller_name, "
            + "CAST (document->>'price' AS NUMERIC) * CAST (document->>'quantity' AS NUMERIC) AS total "
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
            + "AVG( CAST (document->>'quantity' AS NUMERIC) ) AS qty_avg, "
            + "COUNT( CAST (document->>'quantity' AS NUMERIC) ) AS qty_count, "
            + "COUNT(DISTINCT CAST (document->>'quantity' AS NUMERIC) ) AS qty_distinct_count, "
            + "SUM( CAST (document->>'quantity' AS NUMERIC) ) AS qty_sum, "
            + "MIN( CAST (document->>'quantity' AS NUMERIC) ) AS qty_min, "
            + "MAX( CAST (document->>'quantity' AS NUMERIC) ) AS qty_max "
            + "FROM testCollection WHERE CAST (document->'price' AS NUMERIC) = ? "
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
            .addAggregation(IdentifierExpression.of("item"))
            .build();

    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION, query);
    Assertions.assertThrows(UnsupportedOperationException.class, () -> postgresQueryParser.parse());
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
        "SELECT COUNT(DISTINCT CAST (document->>'quantity' AS NUMERIC) ) AS qty_count, "
            + "document->'item' AS item "
            + "FROM testCollection GROUP BY document->'item' "
            + "HAVING COUNT(DISTINCT CAST (document->>'quantity' AS NUMERIC) ) <= ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(10, params.getObjectParams().get(1));
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
        "SELECT COUNT(DISTINCT CAST (document->>'quantity' AS NUMERIC) ) AS qty_count, "
            + "document->'item' AS item, "
            + "document->'price' AS price "
            + "FROM testCollection "
            + "GROUP BY document->'item',document->'price' "
            + "HAVING (COUNT(DISTINCT CAST (document->>'quantity' AS NUMERIC) ) <= ?) "
            + "AND (CAST (document->'price' AS NUMERIC) > ?)",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(10, params.getObjectParams().get(1));
    Assertions.assertEquals(5, params.getObjectParams().get(2));
  }
}
