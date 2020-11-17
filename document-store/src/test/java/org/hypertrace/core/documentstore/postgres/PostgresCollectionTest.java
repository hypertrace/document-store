package org.hypertrace.core.documentstore.postgres;

import org.hypertrace.core.documentstore.Filter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class PostgresCollectionTest {
  
  public static final String ID_KEY = "id";
  private static final String CREATED_AT = "created_at";
  
  private Connection client;
  private PostgresCollection collection;
  private String COLLECTION_NAME = "mytest";
  private String COLUMN_TYPE = "jsonb";
  
  @BeforeEach
  public void setUp() throws SQLException {
    client = mock(Connection.class);
    collection = new PostgresCollection(client, COLLECTION_NAME, COLUMN_TYPE);
  }
  
  @Test
  public void testNonJSONFieldParseSimpleQuery() {
    {
      Filter filter = new Filter(Filter.Op.EQ, ID_KEY, "val1");
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(ID_KEY + " = 'val1'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.GT, ID_KEY, 5);
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(ID_KEY + " > '5'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.GTE, ID_KEY, 5);
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(ID_KEY + " >= '5'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.LT, ID_KEY, 5);
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(ID_KEY + " < '5'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.LTE, ID_KEY, 5);
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(ID_KEY + " <= '5'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.LIKE, ID_KEY, "abc");
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(ID_KEY + " ILIKE '%abc%'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.IN, ID_KEY, List.of("abc", "xyz"));
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(ID_KEY + " IN ('abc', 'xyz')", query);
    }
  }
  
  @Test
  public void testJSONFieldParseSimpleQuery() {
    {
      Filter filter = new Filter(Filter.Op.EQ, "key1", "val1");
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("document->>'key1' = 'val1'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.GT, "key1", 5);
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("document->>'key1' > '5'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.GTE, "key1", 5);
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("document->>'key1' >= '5'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.LT, "key1", 5);
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("document->>'key1' < '5'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.LTE, "key1", 5);
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("document->>'key1' <= '5'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.LIKE, "key1", "abc");
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("document->>'key1' ILIKE '%abc%'", query);
    }
    
    {
      Filter filter = new Filter(Filter.Op.IN, "key1", List.of("abc", "xyz"));
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("document->>'key1' IN ('abc', 'xyz')", query);
    }
    
  }
  
  @Test
  public void testUnsupportedParseQuery() {
    String expectedMessage = "Only Equality predicate is supported";
    {
      Filter filter = new Filter(Filter.Op.EXISTS, "key1", null);
      Exception exception = assertThrows(RuntimeException.class, () -> collection.parseQuery(filter));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }
    {
      Filter filter = new Filter(Filter.Op.NOT_EXISTS, "key1", null);
      Exception exception = assertThrows(RuntimeException.class, () -> collection.parseQuery(filter));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }
    {
      Filter filter = new Filter(Filter.Op.NEQ, "key1", null);
      Exception exception = assertThrows(RuntimeException.class, () -> collection.parseQuery(filter));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }
    {
      Filter filter = new Filter(Filter.Op.CONTAINS, "key1", null);
      Exception exception = assertThrows(RuntimeException.class, () -> collection.parseQuery(filter));
      String actualMessage = exception.getMessage();
      Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }
  }
  
  @Test
  public void testNonJSONFieldParseAndOrQuery() {
    {
      Filter filter =
        new Filter(Filter.Op.EQ, ID_KEY, "val1").and(new Filter(Filter.Op.EQ, CREATED_AT, "val2"));
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(String.format("(%s = 'val1') AND (%s = 'val2')", ID_KEY, CREATED_AT), query);
    }
    
    {
      Filter filter =
        new Filter(Filter.Op.EQ, ID_KEY, "val1").or(new Filter(Filter.Op.EQ, CREATED_AT, "val2"));
      
      String query = collection.parseQuery(filter);
      Assertions.assertEquals(String.format("(%s = 'val1') OR (%s = 'val2')", ID_KEY, CREATED_AT), query);
    }
  }
  
  @Test
  public void testJSONFieldParseAndOrQuery() {
    {
      Filter filter =
        new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("(document->>'key1' = 'val1') AND (document->>'key2' = 'val2')", query);
    }
    
    {
      Filter filter =
        new Filter(Filter.Op.EQ, "key1", "val1").or(new Filter(Filter.Op.EQ, "key2", "val2"));
      
      String query = collection.parseQuery(filter);
      Assertions.assertEquals("(document->>'key1' = 'val1') OR (document->>'key2' = 'val2')", query);
    }
  }
  
  @Test
  public void testNonJSONFieldParseNestedQuery() {
    Filter filter1 =
      new Filter(Filter.Op.EQ, ID_KEY, "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));
    
    Filter filter2 =
      new Filter(Filter.Op.EQ, ID_KEY, "val3").and(new Filter(Filter.Op.EQ, "key4", "val4"));
    
    Filter filter = filter1.or(filter2);
    String query = collection.parseQuery(filter);
    Assertions.assertEquals(String.format("((%s = 'val1') AND (document->>'key2' = 'val2')) " +
      "OR ((%s = 'val3') AND (document->>'key4' = 'val4'))", ID_KEY, ID_KEY), query);
    
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
