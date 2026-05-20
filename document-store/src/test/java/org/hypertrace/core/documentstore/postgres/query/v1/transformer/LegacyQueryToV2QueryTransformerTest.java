package org.hypertrace.core.documentstore.postgres.query.v1.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.SortingSpec;
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
    void transformNullQuery_returnsEmptyV2Query() {
      Query result = transformer.transform(null);

      assertNotNull(result);
      assertTrue(result.getSelections().isEmpty());
      assertTrue(result.getFilter().isEmpty());
      assertTrue(result.getSorts().isEmpty());
      assertTrue(result.getPagination().isEmpty());
    }

    @Test
    void transformEmptyQuery_returnsEmptyV2Query() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query();

      Query result = transformer.transform(legacyQuery);

      assertNotNull(result);
      assertTrue(result.getSelections().isEmpty());
      assertTrue(result.getFilter().isEmpty());
      assertTrue(result.getSorts().isEmpty());
      assertTrue(result.getPagination().isEmpty());
    }
  }

  @Nested
  class TransformSelections {

    @Test
    void transformDirectColumnSelection_createsIdentifierExpression() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "status"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withSelection("status");

      Query result = transformer.transform(legacyQuery);

      assertEquals(1, result.getSelections().size());
      SelectionSpec spec = result.getSelections().get(0);
      assertTrue(spec.getExpression() instanceof IdentifierExpression);
      assertEquals("status", ((IdentifierExpression) spec.getExpression()).getName());
    }

    @Test
    void transformJsonbPathSelection_createsJsonIdentifierExpression() {
      PostgresColumnMetadata jsonbColumnMeta = mock(PostgresColumnMetadata.class);
      when(jsonbColumnMeta.getCanonicalType()).thenReturn(DataType.JSON);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "customAttr.myField"))
          .thenReturn(Optional.empty());
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "customAttr"))
          .thenReturn(Optional.of(jsonbColumnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withSelection("customAttr.myField");

      Query result = transformer.transform(legacyQuery);

      assertEquals(1, result.getSelections().size());
      SelectionSpec spec = result.getSelections().get(0);
      assertTrue(spec.getExpression() instanceof JsonIdentifierExpression);
      JsonIdentifierExpression jsonExpr = (JsonIdentifierExpression) spec.getExpression();
      assertEquals("customAttr", jsonExpr.getColumnName());
      assertEquals(1, jsonExpr.getJsonPath().size());
      assertEquals("myField", jsonExpr.getJsonPath().get(0));
    }

    @Test
    void transformMultipleSelections_createsMultipleExpressions() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "col1"))
          .thenReturn(Optional.of(columnMeta));
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "col2"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withSelection("col1").withSelection("col2");

      Query result = transformer.transform(legacyQuery);

      assertEquals(2, result.getSelections().size());
    }

    @Test
    void transformNullFieldName_throwsException() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query();
      legacyQuery.addSelection(null);

      assertThrows(IllegalArgumentException.class, () -> transformer.transform(legacyQuery));
    }
  }

  @Nested
  class TransformOrderBy {

    @Test
    void transformAscendingOrderBy_createsSortWithAscOrder() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "createdAt"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withOrderBy(new OrderBy("createdAt", true));

      Query result = transformer.transform(legacyQuery);

      assertEquals(1, result.getSorts().size());
      SortingSpec sortSpec = result.getSorts().get(0);
      assertEquals(SortOrder.ASC, sortSpec.getOrder());
      assertTrue(sortSpec.getExpression() instanceof IdentifierExpression);
    }

    @Test
    void transformDescendingOrderBy_createsSortWithDescOrder() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "updatedAt"))
          .thenReturn(Optional.of(columnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query()
              .withOrderBy(new OrderBy("updatedAt", false));

      Query result = transformer.transform(legacyQuery);

      assertEquals(1, result.getSorts().size());
      SortingSpec sortSpec = result.getSorts().get(0);
      assertEquals(SortOrder.DESC, sortSpec.getOrder());
    }

    @Test
    void transformJsonbPathOrderBy_createsJsonIdentifierExpression() {
      PostgresColumnMetadata jsonbColumnMeta = mock(PostgresColumnMetadata.class);
      when(jsonbColumnMeta.getCanonicalType()).thenReturn(DataType.JSON);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "props.priority"))
          .thenReturn(Optional.empty());
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "props"))
          .thenReturn(Optional.of(jsonbColumnMeta));

      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query()
              .withOrderBy(new OrderBy("props.priority", true));

      Query result = transformer.transform(legacyQuery);

      assertEquals(1, result.getSorts().size());
      SortingSpec sortSpec = result.getSorts().get(0);
      assertTrue(sortSpec.getExpression() instanceof JsonIdentifierExpression);
    }
  }

  @Nested
  class TransformPagination {

    @Test
    void transformLimitOnly_createsPaginationWithZeroOffset() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withLimit(10);

      Query result = transformer.transform(legacyQuery);

      assertTrue(result.getPagination().isPresent());
      assertEquals(10, result.getPagination().get().getLimit());
      assertEquals(0, result.getPagination().get().getOffset());
    }

    @Test
    void transformLimitAndOffset_createsPaginationWithBoth() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withLimit(20).withOffset(5);

      Query result = transformer.transform(legacyQuery);

      assertTrue(result.getPagination().isPresent());
      assertEquals(20, result.getPagination().get().getLimit());
      assertEquals(5, result.getPagination().get().getOffset());
    }

    @Test
    void transformNegativeLimit_noPagination() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withLimit(-1);

      Query result = transformer.transform(legacyQuery);

      assertTrue(result.getPagination().isEmpty());
    }

    @Test
    void transformNullLimit_noPagination() {
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query();

      Query result = transformer.transform(legacyQuery);

      assertTrue(result.getPagination().isEmpty());
    }
  }

  @Nested
  class TransformFilter {

    @Test
    void transformSimpleEqFilter_createsRelationalExpression() {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, "status"))
          .thenReturn(Optional.of(columnMeta));

      Filter legacyFilter = new Filter(Filter.Op.EQ, "status", "active");
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withFilter(legacyFilter);

      Query result = transformer.transform(legacyQuery);

      assertTrue(result.getFilter().isPresent());
    }

    @Test
    void transformCompositeAndFilter_createsLogicalExpression() {
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

      Query result = transformer.transform(legacyQuery);

      assertTrue(result.getFilter().isPresent());
    }
  }

  @Nested
  class TransformCompleteQuery {

    @Test
    void transformCompleteQuery_allComponentsTransformed() {
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

      Query result = transformer.transform(legacyQuery);

      assertEquals(2, result.getSelections().size());
      assertTrue(result.getFilter().isPresent());
      assertEquals(1, result.getSorts().size());
      assertTrue(result.getPagination().isPresent());
      assertEquals(50, result.getPagination().get().getLimit());
      assertEquals(10, result.getPagination().get().getOffset());
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
      RelationalExpression rel = transformLeafFilter("labels", DataType.STRING, true);

      assertTrue(
          rel.getLhs() instanceof ArrayIdentifierExpression,
          "text[] column should be wrapped as ArrayIdentifierExpression");
      ArrayIdentifierExpression arr = (ArrayIdentifierExpression) rel.getLhs();
      assertEquals("labels", arr.getName());
      assertEquals(DataType.STRING, arr.getElementDataType());
    }

    @Test
    void bigintArrayColumnInFilterBuildsArrayIdentifierWithLongElementType() {
      RelationalExpression rel = transformLeafFilter("sensitivity", DataType.LONG, true);

      ArrayIdentifierExpression arr = (ArrayIdentifierExpression) rel.getLhs();
      assertEquals(DataType.LONG, arr.getElementDataType());
    }

    @Test
    void doublePrecisionArrayColumnInFilterBuildsArrayIdentifierWithDoubleElementType() {
      RelationalExpression rel = transformLeafFilter("riskScores", DataType.DOUBLE, true);

      ArrayIdentifierExpression arr = (ArrayIdentifierExpression) rel.getLhs();
      assertEquals(DataType.DOUBLE, arr.getElementDataType());
    }

    @Test
    void scalarTextColumnInFilterBuildsTypedScalarIdentifier() {
      RelationalExpression rel = transformLeafFilter("status", DataType.STRING, false);

      assertTrue(rel.getLhs() instanceof IdentifierExpression);
      // Scalar typed identifier is NOT an ArrayIdentifierExpression.
      assertFalse(rel.getLhs() instanceof ArrayIdentifierExpression);
      IdentifierExpression id = (IdentifierExpression) rel.getLhs();
      assertEquals("status", id.getName());
      assertEquals(DataType.STRING, id.getDataType());
    }

    @Test
    void scalarBigintColumnInFilterBuildsTypedScalarIdentifierWithLongType() {
      RelationalExpression rel = transformLeafFilter("statusCode", DataType.LONG, false);

      IdentifierExpression id = (IdentifierExpression) rel.getLhs();
      assertEquals(DataType.LONG, id.getDataType());
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

      Query result = transformer.transform(legacyQuery);

      RelationalExpression rel = (RelationalExpression) result.getFilter().orElseThrow();
      assertTrue(rel.getLhs() instanceof IdentifierExpression);
      assertFalse(rel.getLhs() instanceof ArrayIdentifierExpression);
      assertEquals(DataType.UNSPECIFIED, ((IdentifierExpression) rel.getLhs()).getDataType());
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

      Query result = transformer.transform(legacyQuery);

      RelationalExpression rel = (RelationalExpression) result.getFilter().orElseThrow();
      assertEquals(DataType.UNSPECIFIED, ((IdentifierExpression) rel.getLhs()).getDataType());
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

      Query result = transformer.transform(legacyQuery);

      assertEquals(1, result.getSelections().size());
      SelectionSpec spec = result.getSelections().get(0);
      assertTrue(spec.getExpression() instanceof ArrayIdentifierExpression);
      ArrayIdentifierExpression arr = (ArrayIdentifierExpression) spec.getExpression();
      assertEquals(DataType.STRING, arr.getElementDataType());
    }

    /**
     * Stubs a column with the given canonical type / array-ness, then runs a single-leaf EQ filter
     * on it through the transformer and returns the resulting v2 {@link RelationalExpression}.
     */
    private RelationalExpression transformLeafFilter(
        String columnName, DataType canonicalType, boolean isArray) {
      PostgresColumnMetadata columnMeta = mock(PostgresColumnMetadata.class);
      when(columnMeta.getCanonicalType()).thenReturn(canonicalType);
      when(columnMeta.isArray()).thenReturn(isArray);
      when(schemaRegistry.getColumnOrRefresh(TABLE_NAME, columnName))
          .thenReturn(Optional.of(columnMeta));

      Filter legacyFilter = new Filter(Filter.Op.EQ, columnName, "any");
      org.hypertrace.core.documentstore.Query legacyQuery =
          new org.hypertrace.core.documentstore.Query().withFilter(legacyFilter);

      Query result = transformer.transform(legacyQuery);
      return (RelationalExpression) result.getFilter().orElseThrow();
    }
  }
}
