package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.Collection.UNSUPPORTED_QUERY_OPERATION;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.CREATED_AT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT_ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PostgresQueryParserTest {

  @Test
  void testParseNonCompositeFilter() {
    {
      Filter filter = new Filter(Filter.Op.EQ, ID, "val1");
      String query =
          PostgresUtils.parseNonCompositeFilter(
              filter.getFieldName(),
              PostgresUtils.DOCUMENT_COLUMN,
              filter.getOp().toString(),
              filter.getValue(),
              initParams());
      Assertions.assertEquals(ID + " = ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.NEQ, ID, "val1");
      String query =
          PostgresUtils.parseNonCompositeFilter(
              filter.getFieldName(),
              PostgresUtils.DOCUMENT_COLUMN,
              filter.getOp().toString(),
              filter.getValue(),
              initParams());
      Assertions.assertEquals(ID + " != ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.GT, ID, 5);
      String query =
          PostgresUtils.parseNonCompositeFilter(
              filter.getFieldName(),
              PostgresUtils.DOCUMENT_COLUMN,
              filter.getOp().toString(),
              filter.getValue(),
              initParams());
      Assertions.assertEquals("CAST (" + ID + " AS NUMERIC) > ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.GTE, ID, 5);
      String query =
          PostgresUtils.parseNonCompositeFilter(
              filter.getFieldName(),
              PostgresUtils.DOCUMENT_COLUMN,
              filter.getOp().toString(),
              filter.getValue(),
              initParams());
      Assertions.assertEquals("CAST (" + ID + " AS NUMERIC) >= ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LT, ID, 5);
      String query =
          PostgresUtils.parseNonCompositeFilter(
              filter.getFieldName(),
              PostgresUtils.DOCUMENT_COLUMN,
              filter.getOp().toString(),
              filter.getValue(),
              initParams());
      Assertions.assertEquals("CAST (" + ID + " AS NUMERIC) < ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LTE, ID, 5);
      String query =
          PostgresUtils.parseNonCompositeFilter(
              filter.getFieldName(),
              PostgresUtils.DOCUMENT_COLUMN,
              filter.getOp().toString(),
              filter.getValue(),
              initParams());
      Assertions.assertEquals("CAST (" + ID + " AS NUMERIC) <= ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LIKE, ID, "abc");
      String query =
          PostgresUtils.parseNonCompositeFilter(
              filter.getFieldName(),
              PostgresUtils.DOCUMENT_COLUMN,
              filter.getOp().toString(),
              filter.getValue(),
              initParams());
      Assertions.assertEquals(ID + " ILIKE ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.IN, ID, List.of("abc", "xyz"));
      String query =
          PostgresUtils.parseNonCompositeFilter(
              filter.getFieldName(),
              PostgresUtils.DOCUMENT_COLUMN,
              filter.getOp().toString(),
              filter.getValue(),
              initParams());
      Assertions.assertEquals(ID + " IN (?, ?)", query);
    }
  }

  @Test
  void testParseNonCompositeFilterForJsonField() {
    {
      Filter filter = new Filter(Filter.Op.EQ, "key1", "val1");
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("document->>'key1' = ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.NEQ, "key1", "val1");
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("document->'key1' IS NULL OR document->>'key1' != ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.GT, "key1", 5);
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("CAST (document->>'key1' AS NUMERIC) > ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.GTE, "key1", 5);
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("CAST (document->>'key1' AS NUMERIC) >= ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LT, "key1", 5);
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("CAST (document->>'key1' AS NUMERIC) < ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LTE, "key1", 5);
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("CAST (document->>'key1' AS NUMERIC) <= ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LIKE, "key1", "abc");
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("document->>'key1' ILIKE ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.IN, "key1", List.of("abc", "xyz"));
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("document->>'key1' IN (?, ?)", query);
    }

    {
      Filter filter = new Filter(Op.NOT_IN, "key1", List.of("abc", "xyz"));
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("document->'key1' IS NULL OR document->>'key1' NOT IN (?, ?)", query);
    }

    {
      Filter filter = new Filter(Filter.Op.EQ, DOCUMENT_ID, "k1:k2");
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("document->>'_id' = ?", query);
    }

    {
      Filter filter = new Filter(Filter.Op.EXISTS, "key1.key2", null);
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      System.err.println(query);
      Assertions.assertEquals("document->'key1'->'key2' IS NOT NULL ", query);
    }

    {
      Filter filter = new Filter(Filter.Op.NOT_EXISTS, "key1", null);
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("document->'key1' IS NULL ", query);
    }
  }

  @Test
  void testNonCompositeFilterUnsupportedException() {
    String expectedMessage = UNSUPPORTED_QUERY_OPERATION;
    {
      Filter filter = new Filter(Filter.Op.CONTAINS, "key1", null);
      String expected = String.format(expectedMessage, Filter.Op.CONTAINS);
      Exception exception =
          assertThrows(
              UnsupportedOperationException.class,
              () -> PostgresQueryParser.parseFilter(filter, initParams()));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expected));
    }
  }

  @Test
  void testParseQueryForCompositeFilterWithNullConditions() {
    {
      Filter filter = new Filter(Filter.Op.AND, null, null);
      Assertions.assertNull(PostgresQueryParser.parseFilter(filter, initParams()));
    }
    {
      Filter filter = new Filter(Filter.Op.OR, null, null);
      Assertions.assertNull(PostgresQueryParser.parseFilter(filter, initParams()));
    }
  }

  @Test
  void testParseQueryForCompositeFilter() {
    {
      Filter filter =
          new Filter(Filter.Op.EQ, ID, "val1").and(new Filter(Filter.Op.EQ, CREATED_AT, "val2"));
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals(String.format("(%s = ?) AND (%s = ?)", ID, CREATED_AT), query);
    }

    {
      Filter filter =
          new Filter(Filter.Op.EQ, ID, "val1").or(new Filter(Filter.Op.EQ, CREATED_AT, "val2"));

      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals(String.format("(%s = ?) OR (%s = ?)", ID, CREATED_AT), query);
    }
  }

  @Test
  void testParseQueryForCompositeFilterForJsonField() {
    {
      Filter filter =
          new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));
      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("(document->>'key1' = ?) AND (document->>'key2' = ?)", query);
    }

    {
      Filter filter =
          new Filter(Filter.Op.EQ, "key1", "val1").or(new Filter(Filter.Op.EQ, "key2", "val2"));

      String query = PostgresQueryParser.parseFilter(filter, initParams());
      Assertions.assertEquals("(document->>'key1' = ?) OR (document->>'key2' = ?)", query);
    }
  }

  @Test
  void testParseNestedQuery() {
    Filter filter1 =
        new Filter(Filter.Op.EQ, ID, "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));

    Filter filter2 =
        new Filter(Filter.Op.EQ, ID, "val3").and(new Filter(Filter.Op.EQ, "key4", "val4"));

    Filter filter = filter1.or(filter2);
    String query = PostgresQueryParser.parseFilter(filter, initParams());
    Assertions.assertEquals(
        String.format(
            "((%s = ?) AND (document->>'key2' = ?)) " + "OR ((%s = ?) AND (document->>'key4' = ?))",
            ID, ID),
        query);
  }

  @Test
  void testJSONFieldParseNestedQuery() {
    Filter filter1 =
        new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));

    Filter filter2 =
        new Filter(Filter.Op.EQ, "key3", "val3").and(new Filter(Filter.Op.EQ, "key4", "val4"));

    Filter filter = filter1.or(filter2);
    String query = PostgresQueryParser.parseFilter(filter, initParams());
    Assertions.assertEquals(
        "((document->>'key1' = ?) AND (document->>'key2' = ?)) "
            + "OR ((document->>'key3' = ?) AND (document->>'key4' = ?))",
        query);
  }

  @Test
  void testParseOrderBys() {
    List<OrderBy> orderBys =
        List.of(new OrderBy("key1", true), new OrderBy("key2", false), new OrderBy("key3", true));
    Assertions.assertEquals(
        "document->>'key1' ASC , document->>'key2' DESC , document->>'key3' ASC",
        PostgresQueryParser.parseOrderBys(orderBys));
  }

  @Test
  void testSelectionClause() {
    List<String> selections =
        List.of("id", "identifyingAttributes", "tenantId", "attributes", "type");
    Assertions.assertEquals(
        "id AS \"id\",document->'identifyingAttributes' AS \"identifyingAttributes\",document->'tenantId' AS \"tenantId\",document->'attributes' AS \"attributes\",document->'type' AS \"type\"",
        PostgresQueryParser.parseSelections(selections));
  }

  private Params.Builder initParams() {
    return Params.newBuilder();
  }
}
