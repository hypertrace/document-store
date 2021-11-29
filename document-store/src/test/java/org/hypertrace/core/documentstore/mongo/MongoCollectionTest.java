package org.hypertrace.core.documentstore.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.List;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for utility/helper methods in {@link MongoCollection} */
public class MongoCollectionTest {

  private com.mongodb.client.MongoCollection collection;
  private MongoCollection mongoCollection;

  @BeforeEach
  public void setup() {
    collection = mock(com.mongodb.client.MongoCollection.class);
    mongoCollection = new MongoCollection(collection);

    MongoNamespace namespace = new MongoNamespace("Mongo.test_collection");
    when(collection.getNamespace()).thenReturn(namespace);
  }

  @Test
  public void testLimitInQuery() {
    Query query = new Query();
    query.setLimit(10);

    FindIterable<BasicDBObject> findCursor = mock(FindIterable.class);
    FindIterable<BasicDBObject> cursor = mock(FindIterable.class);
    MongoCursor<BasicDBObject> mongoCursor = mock(MongoCursor.class);

    when(collection.find(any(BasicDBObject.class))).thenReturn(findCursor);
    when(findCursor.projection(any(Bson.class))).thenReturn(cursor);
    when(cursor.cursor()).thenReturn(mongoCursor);
    when(cursor.limit(10)).thenReturn(cursor);
    mongoCollection.search(query);

    verify(cursor, times(1)).limit(10);
  }

  @Test
  public void testNullLimitInQuery() {
    Query query = new Query();
    query.setLimit(null);

    FindIterable<BasicDBObject> findCursor = mock(FindIterable.class);
    FindIterable<BasicDBObject> cursor = mock(FindIterable.class);

    when(collection.find(any(BasicDBObject.class))).thenReturn(findCursor);
    when(findCursor.projection(any(Bson.class))).thenReturn(cursor);
    mongoCollection.search(query);

    verify(cursor, times(0)).limit(anyInt());
  }

  @Test
  public void testOffsetInQuery() {
    Query query = new Query();
    query.setOffset(10);

    FindIterable<BasicDBObject> findCursor = mock(FindIterable.class);
    FindIterable<BasicDBObject> cursor = mock(FindIterable.class);

    when(collection.find(any(BasicDBObject.class))).thenReturn(findCursor);
    when(findCursor.projection(any(Bson.class))).thenReturn(cursor);
    when(cursor.skip(10)).thenReturn(cursor);
    mongoCollection.search(query);

    verify(cursor, times(1)).skip(10);
  }

  @Test
  public void testNullOffsetInQuery() {
    Query query = new Query();
    query.setOffset(null);

    FindIterable<BasicDBObject> findCursor = mock(FindIterable.class);
    FindIterable<BasicDBObject> cursor = mock(FindIterable.class);

    when(collection.find(any(BasicDBObject.class))).thenReturn(findCursor);
    when(findCursor.projection(any(Bson.class))).thenReturn(cursor);
    mongoCollection.search(query);

    verify(cursor, times(0)).skip(anyInt());
  }

  @Test
  public void testOffsetAndLimitInQuery() {
    Query query = new Query();
    query.setLimit(5);
    query.setOffset(10);

    FindIterable<BasicDBObject> findCursor = mock(FindIterable.class);
    FindIterable<BasicDBObject> cursor = mock(FindIterable.class);
    MongoCursor<BasicDBObject> mongoCursor = mock(MongoCursor.class);

    when(collection.find(any(BasicDBObject.class))).thenReturn(findCursor);
    when(findCursor.projection(any(Bson.class))).thenReturn(cursor);
    when(cursor.cursor()).thenReturn(mongoCursor);
    when(cursor.limit(anyInt())).thenReturn(cursor);
    when(cursor.skip(anyInt())).thenReturn(cursor);

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

  @Test
  public void testProjections() {
    Query query = new Query();
    query.addAllSelections(List.of("proj1", "proj2"));

    FindIterable findCursor = mock(FindIterable.class);
    when(collection.find(any(BasicDBObject.class))).thenReturn(findCursor);

    FindIterable projectionCursor = mock(FindIterable.class);
    ArgumentCaptor<Bson> projectionArgumentCaptor = ArgumentCaptor.forClass(Bson.class);
    when(findCursor.projection(projectionArgumentCaptor.capture())).thenReturn(projectionCursor);

    mongoCollection.search(query);
    assertEquals("{\"proj1\": 1, \"proj2\": 1}", projectionArgumentCaptor.getValue().toString());
  }
}
