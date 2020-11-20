package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.PostgresCollection.CREATED_AT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT_ID;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.util.List;
import org.hypertrace.core.documentstore.Filter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PostgresCollectionTest {

  private PostgresCollection collection;

  @BeforeEach
  public void setUp() {
    String COLLECTION_NAME = "mytest";
    Connection client = mock(Connection.class);
    collection = new PostgresCollection(client, COLLECTION_NAME);
  }

  @Test
  public void testParseQueryForNonCompositeFilter() {
    {
      Filter filter = new Filter(Filter.Op.EQ, ID, "val1");
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals(ID + " = 'val1'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.GT, ID, 5);
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals(ID + " > '5'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.GTE, ID, 5);
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals(ID + " >= '5'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LT, ID, 5);
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals(ID + " < '5'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LTE, ID, 5);
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals(ID + " <= '5'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LIKE, ID, "abc");
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals(ID + " ILIKE '%abc%'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.IN, ID, List.of("abc", "xyz"));
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals(ID + " IN ('abc', 'xyz')", query);
    }
  }

  @Test
  public void testParseQueryForNonCompositeFilterForJsonField() {
    {
      Filter filter = new Filter(Filter.Op.EQ, "key1", "val1");
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals("document->>'key1' = 'val1'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.GT, "key1", 5);
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals("document->>'key1' > '5'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.GTE, "key1", 5);
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals("document->>'key1' >= '5'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LT, "key1", 5);
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals("document->>'key1' < '5'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LTE, "key1", 5);
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals("document->>'key1' <= '5'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.LIKE, "key1", "abc");
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals("document->>'key1' ILIKE '%abc%'", query);
    }

    {
      Filter filter = new Filter(Filter.Op.IN, "key1", List.of("abc", "xyz"));
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals("document->>'key1' IN ('abc', 'xyz')", query);
    }

    {
      Filter filter = new Filter(Filter.Op.EQ, DOCUMENT_ID, "k1:k2");
      String query = collection.parseQueryForNonCompositeFilter(filter);
      Assertions.assertEquals("document->>'_id' = 'k1:k2'", query);
    }

  }

  @Test
  public void testParseQueryForNonCompositeFilterUnsupportedException() {
    String expectedMessage = "Only Equality predicate is supported";
    {
      Filter filter = new Filter(Filter.Op.EXISTS, "key1", null);
      Exception exception = assertThrows(UnsupportedOperationException.class,
          () -> collection.parseQueryForNonCompositeFilter(filter));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }
    {
      Filter filter = new Filter(Filter.Op.NOT_EXISTS, "key1", null);
      Exception exception = assertThrows(UnsupportedOperationException.class,
          () -> collection.parseQueryForNonCompositeFilter(filter));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }
    {
      Filter filter = new Filter(Filter.Op.NEQ, "key1", null);
      Exception exception = assertThrows(UnsupportedOperationException.class,
          () -> collection.parseQueryForNonCompositeFilter(filter));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }
    {
      Filter filter = new Filter(Filter.Op.CONTAINS, "key1", null);
      Exception exception = assertThrows(UnsupportedOperationException.class,
          () -> collection.parseQueryForNonCompositeFilter(filter));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }
  }

  @Test
  public void testParseQueryForCompositeFilterWithNullConditions() {
    {
      Filter filter = new Filter(Filter.Op.AND, null, null);
      Assertions.assertNull(collection.parseQuery(filter));
    }
    {
      Filter filter = new Filter(Filter.Op.OR, null, null);
      Assertions.assertNull(collection.parseQuery(filter));
    }
  }

  @Test
  public void testParseQueryForCompositeFilter() {
    {
      Filter filter =
          new Filter(Filter.Op.EQ, ID, "val1").and(new Filter(Filter.Op.EQ, CREATED_AT, "val2"));
      String query = collection.parseQueryForCompositeFilter(filter);
      Assertions
          .assertEquals(String.format("(%s = 'val1') AND (%s = 'val2')", ID, CREATED_AT), query);
    }

    {
      Filter filter =
          new Filter(Filter.Op.EQ, ID, "val1").or(new Filter(Filter.Op.EQ, CREATED_AT, "val2"));

      String query = collection.parseQueryForCompositeFilter(filter);
      Assertions
          .assertEquals(String.format("(%s = 'val1') OR (%s = 'val2')", ID, CREATED_AT), query);
    }
  }

  @Test
  public void testParseQueryForCompositeFilterForJsonField() {
    {
      Filter filter =
          new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));
      String query = collection.parseQueryForCompositeFilter(filter);
      Assertions
          .assertEquals("(document->>'key1' = 'val1') AND (document->>'key2' = 'val2')", query);
    }

    {
      Filter filter =
          new Filter(Filter.Op.EQ, "key1", "val1").or(new Filter(Filter.Op.EQ, "key2", "val2"));

      String query = collection.parseQueryForCompositeFilter(filter);
      Assertions
          .assertEquals("(document->>'key1' = 'val1') OR (document->>'key2' = 'val2')", query);
    }
  }

  @Test
  public void testParseNestedQuery() {
    Filter filter1 =
        new Filter(Filter.Op.EQ, ID, "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));

    Filter filter2 =
        new Filter(Filter.Op.EQ, ID, "val3").and(new Filter(Filter.Op.EQ, "key4", "val4"));

    Filter filter = filter1.or(filter2);
    String query = collection.parseQuery(filter);
    Assertions.assertEquals(String.format("((%s = 'val1') AND (document->>'key2' = 'val2')) " +
        "OR ((%s = 'val3') AND (document->>'key4' = 'val4'))", ID, ID), query);

  }

  @Test
  public void testJSONFieldParseNestedQuery() {
    Filter filter1 =
        new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));

    Filter filter2 =
        new Filter(Filter.Op.EQ, "key3", "val3").and(new Filter(Filter.Op.EQ, "key4", "val4"));

    Filter filter = filter1.or(filter2);
    String query = collection.parseQuery(filter);
    Assertions.assertEquals("((document->>'key1' = 'val1') AND (document->>'key2' = 'val2')) " +
        "OR ((document->>'key3' = 'val3') AND (document->>'key4' = 'val4'))", query);

  }

}
