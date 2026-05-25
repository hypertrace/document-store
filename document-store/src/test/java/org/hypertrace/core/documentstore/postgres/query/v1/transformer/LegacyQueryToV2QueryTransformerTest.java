package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class LegacyQueryToV2QueryTransformerTest {

  private static final String TABLE_NAME = "test_table";
  private SchemaRegistry<PostgresColumnMetadata> schemaRegistry;
  private LegacyQueryToV2QueryTransformer transformer;

  @BeforeEach
  void setUp() {
    schemaRegistry = mock(SchemaRegistry.class);
    transformer = new LegacyQueryToV2QueryTransformer(schemaRegistry, TABLE_NAME);
  }

  @Nested
  class TransformNullOrEmptyQuery {

    @Test
    void transformNullQueryReturnsEmptyV2Query() {
      assertEquals(Query.builder().build(), transformer.transform(null));
    }

    @Test
    void transformEmptyQueryReturnsEmptyV2Query() {
      assertEquals(
          Query.builder().build(),
          transformer.transform(new org.hypertrace.core.documentstore.Query()));
    }
  }

  @Nested
  class TransformSelections {

    @Test
    void transformDirectColumnSelectionCreatesIdentifierExpression() {
      // Stubbed column has no canonical type -> factory yields a bare IdentifierExpression.
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "status"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withSelection("status");

      Query expected = Query.builder().addSelection(IdentifierExpression.of("status")).build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void transformJsonbPathSelectionCreatesJsonIdentifierExpression() {
      // "customAttr" resolves to a JSON column; ".myField" becomes the JSON path. Selections
      // default JsonFieldType to STRING (no value available to infer from).
      PostgresColumnMetadata jsonbColumnMeta = mock(PostgresColumnMetadata.class);
      when(jsonbColumnMeta.getCanonicalType()).thenReturn(DataType.JSON);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "customAttr.myField"))
          .thenReturn(Optional.empty());
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "customAttr"))
          .thenReturn(Optional.of(jsonbColumnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withSelection("customAttr.myField");

      Query expected =
          Query.builder()
              .addSelection(
                  JsonIdentifierExpression.of("customAttr", JsonFieldType.STRING, "myField"))
              .build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void transformMultipleSelectionsCreatesMultipleExpressions() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "col1"))
          .thenReturn(Optional.of(columnMeta));
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "col2"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withSelection("col1").withSelection("col2");

      Query expected =
          Query.builder()
              .addSelection(IdentifierExpression.of("col1"))
              .addSelection(IdentifierExpression.of("col2"))
              .build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void transformNullFieldNameThrowsException() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query();
      legacyQuery.addSelection(null);

      assertThrows(IllegalArgumentException.class, () -> transformer.transform(legacyQuery));
    }
  }

  @Nested
  class TransformOrderBy {

    @Test
    void transformAscendingOrderByCreatesSortWithAscOrder() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "createdAt"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withOrderBy(new OrderBy("createdAt", true));

      Query expected = Query.builder().addSort(IdentifierExpression.of("createdAt"), ASC).build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void transformDescendingOrderByCreatesSortWithDescOrder() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "updatedAt"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query()
              .withOrderBy(new OrderBy("updatedAt", false));

      Query expected = Query.builder().addSort(IdentifierExpression.of("updatedAt"), DESC).build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void transformJsonbPathOrderByCreatesJsonIdentifierExpression() {
      PostgresColumnMetadata jsonbColumnMeta = mock(PostgresColumnMetadata.class);
      when(jsonbColumnMeta.getCanonicalType()).thenReturn(DataType.JSON);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "props.priority"))
          .thenReturn(Optional.empty());
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "props"))
          .thenReturn(Optional.of(jsonbColumnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query()
              .withOrderBy(new OrderBy("props.priority", true));

      Query expected =
          Query.builder()
              .addSort(JsonIdentifierExpression.of("props", JsonFieldType.STRING, "priority"), ASC)
              .build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }
  }

  @Nested
  class TransformPagination {

    @Test
    void transformLimitOnlyCreatesPaginationWithZeroOffset() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withLimit(10);

      Query expected =
          Query.builder().setPagination(Pagination.builder().offset(0).limit(10).build()).build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void transformLimitAndOffsetCreatesPaginationWithBoth() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withLimit(20).withOffset(5);

      Query expected =
          Query.builder().setPagination(Pagination.builder().offset(5).limit(20).build()).build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void transformNegativeLimitNoPagination() {
      // Negative limits are dropped silently; nothing else is set, so the result is an empty
      // v2 Query.
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withLimit(-1);

      assertEquals(Query.builder().build(), transformer.transform(legacyQuery));
    }

    @Test
    void transformNullLimitNoPagination() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query();

      assertEquals(Query.builder().build(), transformer.transform(legacyQuery));
    }
  }

  @Nested
  class TransformFilter {

    @Test
    void transformSimpleEqFilterCreatesRelationalExpression() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "status"))
          .thenReturn(Optional.of(columnMeta));

      Filter legacyFilter = new Filter(Filter.Op.EQ, "status", "active");
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withFilter(legacyFilter);

      Query expected =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("status"), EQ, ConstantExpression.of("active")))
              .build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void transformCompositeAndFilterCreatesLogicalExpression() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(anyString(), anyString()))
          .thenReturn(Optional.of(columnMeta));

      Filter filter1 = new Filter(Filter.Op.EQ, "status", "active");
      Filter filter2 = new Filter(Filter.Op.GT, "count", 10);
      Filter compositeFilter = new Filter();
      compositeFilter.setOp(Filter.Op.AND);
      compositeFilter.setChildFilters(new Filter[] {filter1, filter2});

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withFilter(compositeFilter);

      Query expected =
          Query.builder()
              .setFilter(
                  LogicalExpression.and(
                      RelationalExpression.of(
                          IdentifierExpression.of("status"), EQ, ConstantExpression.of("active")),
                      RelationalExpression.of(
                          IdentifierExpression.of("count"), GT, ConstantExpression.of(10))))
              .build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }
  }

  @Nested
  class TransformCompleteQuery {

    @Test
    void transformCompleteQueryAllComponentsTransformed() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(eq(TABLE_NAME), anyString()))
          .thenReturn(Optional.of(columnMeta));

      Filter legacyFilter = new Filter(Filter.Op.EQ, "status", "active");
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query()
              .withSelection("id")
              .withSelection("name")
              .withFilter(legacyFilter)
              .withOrderBy(new OrderBy("createdAt", false))
              .withLimit(50)
              .withOffset(10);

      Query expected =
          Query.builder()
              .addSelection(IdentifierExpression.of("id"))
              .addSelection(IdentifierExpression.of("name"))
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("status"), EQ, ConstantExpression.of("active")))
              .addSort(IdentifierExpression.of("createdAt"), DESC)
              .setPagination(Pagination.builder().offset(10).limit(50).build())
              .build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }
  }

  /**
   * Tests that verify the transformer wraps directly-resolved columns in <em>typed</em> identifier
   * expressions (so downstream Postgres parsers can emit array-aware SQL like {@code col && ?},
   * {@code col @> ARRAY[?]::int8[]}, {@code col = ANY(?)}) when the {@link PostgresColumnMetadata}
   * carries a non-UNSPECIFIED canonical type.
   *
   * <p>Each test fixes a known column shape via Mockito stubbing and asserts the resulting
   * expression's runtime class and (for arrays) element type.
   */
  @Nested
  class TypedAndArrayColumnIdentifiers {

    @Test
    void textArrayColumnInFilterBuildsArrayIdentifierWithStringElementType() {
      Query expected = leafEqFilterQuery(ArrayIdentifierExpression.ofStrings("labels"));
      assertEquals(expected, transformLeafFilter("labels", DataType.STRING, true));
    }

    @Test
    void bigintArrayColumnInFilterBuildsArrayIdentifierWithLongElementType() {
      Query expected = leafEqFilterQuery(ArrayIdentifierExpression.ofLongs("sensitivity"));
      assertEquals(expected, transformLeafFilter("sensitivity", DataType.LONG, true));
    }

    @Test
    void doublePrecisionArrayColumnInFilterBuildsArrayIdentifierWithDoubleElementType() {
      Query expected = leafEqFilterQuery(ArrayIdentifierExpression.ofDoubles("riskScores"));
      assertEquals(expected, transformLeafFilter("riskScores", DataType.DOUBLE, true));
    }

    @Test
    void scalarTextColumnInFilterBuildsTypedScalarIdentifier() {
      Query expected = leafEqFilterQuery(IdentifierExpression.ofString("status"));
      assertEquals(expected, transformLeafFilter("status", DataType.STRING, false));
    }

    @Test
    void scalarBigintColumnInFilterBuildsTypedScalarIdentifierWithLongType() {
      Query expected = leafEqFilterQuery(IdentifierExpression.ofLong("statusCode"));
      assertEquals(expected, transformLeafFilter("statusCode", DataType.LONG, false));
    }

    @Test
    void unspecifiedTypeColumnInFilterFallsBackToUntypedIdentifier() {
      // No canonical type stubbed -> Mockito returns null, transformer must fall back to untyped.
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "legacyCol"))
          .thenReturn(Optional.of(columnMeta));

      Filter legacyFilter = new Filter(Filter.Op.EQ, "legacyCol", "x");
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withFilter(legacyFilter);

      Query expected =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("legacyCol"), EQ, ConstantExpression.of("x")))
              .build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void jsonColumnDirectlySelectedDoesNotGetWrappedAsTypedScalar() {
      // JSON columns are handled separately via JsonIdentifierExpression elsewhere; the factory
      // must NOT promote them to typed scalar IdentifierExpression.
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(columnMeta.getCanonicalType()).thenReturn(DataType.JSON);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "props"))
          .thenReturn(Optional.of(columnMeta));

      Filter legacyFilter = new Filter(Filter.Op.EQ, "props", "x");
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withFilter(legacyFilter);

      Query expected =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("props"), EQ, ConstantExpression.of("x")))
              .build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    @Test
    void textArrayColumnInSelectionBuildsArrayIdentifierExpression() {
      // Verifies the LegacyQueryToV2QueryTransformer.createIdentifierExpression path used for
      // selections/orderBy (separate code site from the filter path above).
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(columnMeta.getCanonicalType()).thenReturn(DataType.STRING);
      when(columnMeta.isArray()).thenReturn(true);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "labels"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withSelection("labels");

      Query expected =
          Query.builder().addSelection(ArrayIdentifierExpression.ofStrings("labels")).build();
      assertEquals(expected, transformer.transform(legacyQuery));
    }

    private Query transformLeafFilter(String columnName, DataType canonicalType, boolean isArray) {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(columnMeta.getCanonicalType()).thenReturn(canonicalType);
      when(columnMeta.isArray()).thenReturn(isArray);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, columnName))
          .thenReturn(Optional.of(columnMeta));

      Filter legacyFilter = new Filter(Filter.Op.EQ, columnName, "any");
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withFilter(legacyFilter);

      return transformer.transform(legacyQuery);
    }

    /**
     * Builds the expected v2 Query for a single-leaf {@code EQ <lhs> "any"} filter -- the shape
     * produced by {@link #transformLeafFilter}.
     */
    private Query leafEqFilterQuery(
        org.hypertrace.core.documentstore.expression.type.SelectTypeExpression lhs) {
      return Query.builder()
          .setFilter(RelationalExpression.of(lhs, EQ, ConstantExpression.of("any")))
          .build();
    }
  }

  @Test
  void inferJsonFieldTypeListOfStringsYieldsStringNotStringArray() {
    PostgresColumnMetadata propsMeta = mock(PostgresColumnMetadata.class);
    when(propsMeta.getCanonicalType()).thenReturn(DataType.JSON);
    when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "props.brand")).thenReturn(Optional.empty());
    when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "props")).thenReturn(Optional.of(propsMeta));

    org.hypertrace.core.documentstore.Query legacyQuery =
        new org.hypertrace.core.documentstore.Query()
            .withFilter(new Filter(Filter.Op.IN, "props.brand", List.of("Dettol", "Lifebuoy")));

    Query expected =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    // STRING (not STRING_ARRAY) -- documents the legacy-API gap.
                    JsonIdentifierExpression.of("props", JsonFieldType.STRING, "brand"),
                    IN,
                    ConstantExpression.ofStrings(List.of("Dettol", "Lifebuoy"))))
            .build();
    assertEquals(expected, transformer.transform(legacyQuery));
  }
}
