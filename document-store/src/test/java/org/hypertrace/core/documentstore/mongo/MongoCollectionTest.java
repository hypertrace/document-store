package org.hypertrace.core.documentstore.mongo;

import static java.util.Collections.emptyList;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.hypertrace.core.documentstore.util.TestUtil.assertJsonEquals;
import static org.hypertrace.core.documentstore.util.TestUtil.readBasicDBObject;
import static org.hypertrace.core.documentstore.util.TestUtil.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Unit tests for utility/helper methods in {@link MongoCollection} */
public class MongoCollectionTest {

  private com.mongodb.client.MongoCollection collection;
  private MongoCollection mongoCollection;
  private Clock mockClock;

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @BeforeEach
  public void setup() {
    collection = mock(com.mongodb.client.MongoCollection.class);
    mockClock = mock(Clock.class);

    try (final MockedStatic<Clock> clockMock = Mockito.mockStatic(Clock.class)) {
      clockMock.when(Clock::systemUTC).thenReturn(mockClock);
      mongoCollection = new MongoCollection(collection);
    }

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

  @Test
  void testUpdateAtomicWithFilter() throws IOException {
    final org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"), EQ, ConstantExpression.of("Soap")))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("date"),
                            LT,
                            ConstantExpression.of("2022-08-09T18:53:17Z")))
                    .build())
            .addSort(SortingSpec.of(IdentifierExpression.of("price"), ASC))
            .addSort(SortingSpec.of(IdentifierExpression.of("date"), DESC))
            .addSelection(IdentifierExpression.of("quantity"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("date"))
            .addSelection(IdentifierExpression.of("props"))
            .build();
    final SubDocumentUpdate dateUpdate = SubDocumentUpdate.of("date", "2022-08-09T18:53:17Z");
    final SubDocumentUpdate quantityUpdate = SubDocumentUpdate.of("quantity", 1000);
    final SubDocumentUpdate propsUpdate =
        SubDocumentUpdate.of(
            "props", SubDocumentValue.of(new JSONDocument("{\"brand\": \"Dettol\"}")));

    final BasicDBObject setObject = readBasicDBObject("atomic_read_and_update/set_object.json");
    final BasicDBObject selections = readBasicDBObject("atomic_read_and_update/selection.json");
    final BasicDBObject sort = readBasicDBObject("atomic_read_and_update/sort.json");
    final BasicDBObject filter = readBasicDBObject("atomic_read_and_update/filter.json");
    final BasicDBObject response = readBasicDBObject("atomic_read_and_update/response.json");

    final ArgumentCaptor<FindOneAndUpdateOptions> options =
        ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);

    when(mockClock.millis()).thenReturn(1660721309000L);
    when(collection.findOneAndUpdate(eq(filter), eq(setObject), options.capture()))
        .thenReturn(response);

    final Optional<Document> result =
        mongoCollection.update(query, List.of(dateUpdate, quantityUpdate, propsUpdate));

    assertTrue(result.isPresent());
    assertJsonEquals(
        readFileFromResource("atomic_read_and_update/response.json").orElseThrow(),
        result.get().toJson());
    assertEquals(selections, options.getValue().getProjection());
    assertEquals(sort, options.getValue().getSort());

    verify(collection, times(1))
        .findOneAndUpdate(eq(filter), eq(setObject), any(FindOneAndUpdateOptions.class));
  }

  @Test
  void testAtomicUpdateWithoutUpdates() {
    assertThrows(
        IOException.class,
        () ->
            mongoCollection.update(
                org.hypertrace.core.documentstore.query.Query.builder().build(), emptyList()));
  }
}
