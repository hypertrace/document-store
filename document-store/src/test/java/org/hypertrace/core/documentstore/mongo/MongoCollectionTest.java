package org.hypertrace.core.documentstore.mongo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import java.util.List;
import java.util.Map;

import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for utility/helper methods in {@link MongoCollection}
 */
public class MongoCollectionTest {

    private com.mongodb.client.MongoCollection collection;
    private MongoCollection mongoCollection;

    @BeforeEach
    public void setup() {
        collection = mock(com.mongodb.client.MongoCollection.class);
        mongoCollection = new MongoCollection(collection);
    }

    @Test
    public void testParseSimpleQuery() {
        {
            Filter filter = new Filter(Filter.Op.EQ, "key1", "val1");
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals("val1", query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.GT, "key1", 5);
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(new BasicDBObject("$gt", 5), query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.GTE, "key1", 5);
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(new BasicDBObject("$gte", 5), query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.LT, "key1", 5);
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(new BasicDBObject("$lt", 5), query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.LTE, "key1", 5);
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(new BasicDBObject("$lte", 5), query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.EXISTS, "key1", null);
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(new BasicDBObject("$exists", true), query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.NOT_EXISTS, "key1", null);
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(new BasicDBObject("$exists", false), query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.LIKE, "key1", ".*abc");
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(
                    new BasicDBObject("$regex", ".*abc").append("$options", "i"), query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.IN, "key1", List.of("abc", "xyz"));
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(new BasicDBObject("$in", List.of("abc", "xyz")), query.get("key1"));
        }

        {
            Filter filter = new Filter(Filter.Op.CONTAINS, "key1", "abc");
            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertEquals(new BasicDBObject("$elemMatch", "abc"), query.get("key1"));
        }
    }

    @Test
    public void testParseAndOrQuery() {
        {
            Filter filter =
                    new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));

            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertTrue(query.get("$and") instanceof List);
            Assertions.assertTrue(
                    ((List) query.get("$and"))
                            .containsAll(List.of(Map.of("key1", "val1"), Map.of("key2", "val2"))));
        }

        {
            Filter filter =
                    new Filter(Filter.Op.EQ, "key1", "val1").or(new Filter(Filter.Op.EQ, "key2", "val2"));

            Map<String, Object> query = mongoCollection.parseQuery(filter);
            Assertions.assertTrue(query.get("$or") instanceof List);
            Assertions.assertTrue(
                    ((List) query.get("$or"))
                            .containsAll(List.of(Map.of("key1", "val1"), Map.of("key2", "val2"))));
        }
    }

    @Test
    public void testParseNestedQuery() {
        Filter filter1 =
                new Filter(Filter.Op.EQ, "key1", "val1").and(new Filter(Filter.Op.EQ, "key2", "val2"));

        Filter filter2 =
                new Filter(Filter.Op.EQ, "key3", "val3").and(new Filter(Filter.Op.EQ, "key4", "val4"));

        Filter filter = filter1.or(filter2);
        Map<String, Object> query = mongoCollection.parseQuery(filter);
        Assertions.assertEquals(2, ((List) query.get("$or")).size());
        Assertions.assertTrue(
                ((List) ((Map) ((List) query.get("$or")).get(0)).get("$and"))
                        .containsAll(List.of(Map.of("key1", "val1"), Map.of("key2", "val2"))));
        Assertions.assertTrue(
                ((List) ((Map) ((List) query.get("$or")).get(1)).get("$and"))
                        .containsAll(List.of(Map.of("key3", "val3"), Map.of("key4", "val4"))));
    }

    @Test
    public void testLimitInQuery() {
        Query query = new Query();
        query.setLimit(10);

        FindIterable<BasicDBObject> cursor = mock(FindIterable.class);
        MongoCursor<BasicDBObject> mongoCursor = mock(MongoCursor.class);
        when(collection.find(any(BasicDBObject.class))).thenReturn(cursor);
        when(cursor.cursor()).thenReturn(mongoCursor);
        when(cursor.limit(10)).thenReturn(cursor);
        mongoCollection.search(query);

        verify(cursor, times(1)).limit(10);
    }

    @Test
    public void testNullLimitInQuery() {
        Query query = new Query();
        query.setLimit(null);

        FindIterable<BasicDBObject> cursor = mock(FindIterable.class);
        when(collection.find(any(BasicDBObject.class))).thenReturn(cursor);
        mongoCollection.search(query);

        verify(cursor, times(0)).limit(anyInt());
    }

    @Test
    public void testOffsetInQuery() {
        Query query = new Query();
        query.setOffset(10);

        FindIterable<BasicDBObject> cursor = mock(FindIterable.class);
        when(collection.find(any(BasicDBObject.class))).thenReturn(cursor);
        when(cursor.skip(10)).thenReturn(cursor);
        mongoCollection.search(query);

        verify(cursor, times(1)).skip(10);
    }

    @Test
    public void testNullOffsetInQuery() {
        Query query = new Query();
        query.setOffset(null);

        FindIterable cursor = mock(FindIterable.class);
        when(collection.find(any(BasicDBObject.class))).thenReturn(cursor);
        mongoCollection.search(query);

        verify(cursor, times(0)).skip(anyInt());
    }

    @Test
    public void testOffsetAndLimitInQuery() {
        Query query = new Query();
        query.setLimit(5);
        query.setOffset(10);

        FindIterable cursor = mock(FindIterable.class);
        when(cursor.limit(anyInt())).thenReturn(cursor);
        when(cursor.skip(anyInt())).thenReturn(cursor);

        when(collection.find(any(BasicDBObject.class))).thenReturn(cursor);
        mongoCollection.search(query);

        verify(cursor, times(1)).skip(10);
        verify(cursor, times(1)).limit(5);
    }

    @Test
    public void testTotalWithQuery() {
        Query query = new Query();
        mongoCollection.total(query);
        verify(collection, times(1)).countDocuments(any(BasicDBObject.class));
    }

    @Test
    public void testTotalWithFilter() {
        Query query = new Query();
        Filter filter = new Filter(Filter.Op.EQ, "key1", "val1");
        query.setFilter(filter);

        mongoCollection.total(query);
        verify(collection, times(1)).countDocuments(any(BasicDBObject.class));
    }
}
