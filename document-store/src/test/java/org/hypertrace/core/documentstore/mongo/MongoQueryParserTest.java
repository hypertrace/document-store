package org.hypertrace.core.documentstore.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.BasicDBObject;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.OrderBy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MongoQueryParserTest {

  @Test
  void testParseSimpleQuery() {
    {
      Filter filter = new Filter(Filter.Op.EQ, "key1", "val1");
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals("val1", query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.GT, "key1", 5);
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$gt", 5), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.GTE, "key1", 5);
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$gte", 5), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.LT, "key1", 5);
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$lt", 5), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.LTE, "key1", 5);
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$lte", 5), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.EXISTS, "key1", null);
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$exists", true), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.NOT_EXISTS, "key1", null);
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$exists", false), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.LIKE, "key1", ".*abc");
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$regex", ".*abc").append("$options", "i"), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.IN, "key1", List.of("abc", "xyz"));
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$in", List.of("abc", "xyz")), query.get("key1"));
    }

    {
      Filter filter = new Filter(Op.NOT_IN, "key1", List.of("abc", "xyz"));
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$nin", List.of("abc", "xyz")), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.CONTAINS, "key1", "abc");
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      assertEquals(new BasicDBObject("$elemMatch", "abc"), query.get("key1"));
    }

    {
      Filter filter = new Filter(Filter.Op.NEQ, "key1", "abc");
      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      BasicDBObject notEquals = new BasicDBObject();
      notEquals.append("$ne", "abc");
      Assertions.assertEquals(notEquals, query.get("key1"));
    }
  }

  @Test
  void testParseAndOrQuery() {
    {
      Filter filter =
          new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));

      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      Assertions.assertTrue(query.get("$and") instanceof List);
      Assertions.assertTrue(
          ((List) query.get("$and"))
              .containsAll(List.of(Map.of("key1", "val1"), Map.of("key2", "val2"))));
    }

    {
      Filter filter =
          new Filter(Filter.Op.EQ, "key1", "val1").or(new Filter(Filter.Op.EQ, "key2", "val2"));

      Map<String, Object> query = MongoQueryParser.parseFilter(filter);
      Assertions.assertTrue(query.get("$or") instanceof List);
      Assertions.assertTrue(
          ((List) query.get("$or"))
              .containsAll(List.of(Map.of("key1", "val1"), Map.of("key2", "val2"))));
    }
  }

  @Test
  void testParseNestedQuery() {
    Filter filter1 =
        new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));

    Filter filter2 =
        new Filter(Filter.Op.EQ, "key3", "val3").and(new Filter(Filter.Op.EQ, "key4", "val4"));

    Filter filter = filter1.or(filter2);
    Map<String, Object> query = MongoQueryParser.parseFilter(filter);
    assertEquals(2, ((List) query.get("$or")).size());
    Assertions.assertTrue(
        ((List) ((Map) ((List) query.get("$or")).get(0)).get("$and"))
            .containsAll(List.of(Map.of("key1", "val1"), Map.of("key2", "val2"))));
    Assertions.assertTrue(
        ((List) ((Map) ((List) query.get("$or")).get(1)).get("$and"))
            .containsAll(List.of(Map.of("key3", "val3"), Map.of("key4", "val4"))));
  }

  @Test
  void testParseOrderByQuery() {
    List<OrderBy> orderBys =
        List.of(new OrderBy("key1", true), new OrderBy("key2", false), new OrderBy("key3", true));
    assertEquals(
        Map.of("key1", 1, "key2", -1, "key3", 1), MongoQueryParser.parseOrderBys(orderBys));
  }
}
