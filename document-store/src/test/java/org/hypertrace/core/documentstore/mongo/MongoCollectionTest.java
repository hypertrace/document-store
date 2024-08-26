package org.hypertrace.core.documentstore.mongo;

import static com.mongodb.client.model.ReturnDocument.AFTER;
import static java.util.Collections.emptyList;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.core.documentstore.util.TestUtil.assertJsonEquals;
import static org.hypertrace.core.documentstore.util.TestUtil.readBasicDBObject;
import static org.hypertrace.core.documentstore.util.TestUtil.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.result.UpdateResult;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.model.config.AggregatePipelineMode;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

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
    ConnectionConfig connectionConfig = mock(ConnectionConfig.class);
    when(connectionConfig.aggregationPipelineMode())
        .thenReturn(AggregatePipelineMode.SORT_OPTIMIZED_IF_POSSIBLE);
    try (final MockedStatic<Clock> clockMock = mockStatic(Clock.class)) {
      clockMock.when(Clock::systemUTC).thenReturn(mockClock);
      mongoCollection = new MongoCollection(collection, connectionConfig);
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

  @Nested
  class CreateOrReplaceTest {
    @Test
    void testCreateOrReplace() throws IOException {
      final Key key = Key.from("some-key");
      final Document document = new JSONDocument("{\"planet\": \"Mars\"}");
      @SuppressWarnings("unchecked")
      final ArgumentCaptor<List<BasicDBObject>> valueCaptor = ArgumentCaptor.forClass(List.class);

      when(collection.updateOne(
              eq(new BasicDBObject("_id", key.toString())),
              valueCaptor.capture(),
              any(com.mongodb.client.model.UpdateOptions.class)))
          .thenReturn(UpdateResult.acknowledged(0, 1L, new BsonString("some-key")));

      assertTrue(mongoCollection.createOrReplace(key, document));
      final List<BasicDBObject> value = valueCaptor.getValue();
      final JsonNode node = new ObjectMapper().readTree(value.get(1).get("$set").toString());

      assertEquals("Mars", node.get("planet").textValue());
      assertEquals(key.toString(), node.get("_id").textValue());
      assertTrue(node.get("lastUpdatedTime").longValue() - System.currentTimeMillis() < 1000);
    }
  }

  @Nested
  class UpdateTest {
    private final org.hypertrace.core.documentstore.query.Query query =
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
    private final SubDocumentUpdate dateUpdate =
        SubDocumentUpdate.of("date", "2022-08-09T18:53:17Z");
    private final SubDocumentUpdate quantityUpdate = SubDocumentUpdate.of("quantity", 1000);
    private final SubDocumentUpdate propsUpdate =
        SubDocumentUpdate.of(
            "props", SubDocumentValue.of(new JSONDocument("{\"brand\": \"Dettol\"}")));
    private final BasicDBObject setObject =
        readBasicDBObject("atomic_read_and_update/set_object.json");
    private final BasicDBObject selections =
        readBasicDBObject("atomic_read_and_update/selection.json");
    private final BasicDBObject sort = readBasicDBObject("atomic_read_and_update/sort.json");
    private final BasicDBObject filter = readBasicDBObject("atomic_read_and_update/filter.json");
    private final BasicDBObject response =
        readBasicDBObject("atomic_read_and_update/response.json");

    UpdateTest() throws IOException {}

    @Test
    void testUpdateAtomicWithFilter() throws IOException {
      final ArgumentCaptor<FindOneAndUpdateOptions> options =
          ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);

      when(mockClock.millis()).thenReturn(1660721309000L);
      when(collection.findOneAndUpdate(eq(filter), eq(setObject), options.capture()))
          .thenReturn(response);

      final Optional<Document> result =
          mongoCollection.update(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate),
              UpdateOptions.DEFAULT_UPDATE_OPTIONS);

      assertTrue(result.isPresent());
      assertJsonEquals(
          readFileFromResource("atomic_read_and_update/response.json").orElseThrow(),
          result.get().toJson());
      assertEquals(selections, options.getValue().getProjection());
      assertEquals(sort, options.getValue().getSort());
      assertEquals(AFTER, options.getValue().getReturnDocument());

      verify(collection, times(1))
          .findOneAndUpdate(eq(filter), eq(setObject), any(FindOneAndUpdateOptions.class));
    }

    @Test
    void testUpdateAtomicWithFilter_getNone() throws IOException {
      final ArgumentCaptor<FindOneAndUpdateOptions> options =
          ArgumentCaptor.forClass(FindOneAndUpdateOptions.class);

      when(mockClock.millis()).thenReturn(1660721309000L);
      when(collection.findOneAndUpdate(eq(filter), eq(setObject), options.capture()))
          .thenReturn(response);

      final Optional<Document> result =
          mongoCollection.update(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate),
              UpdateOptions.builder().returnDocumentType(NONE).build());

      assertFalse(result.isPresent());

      assertEquals(selections, options.getValue().getProjection());
      assertEquals(sort, options.getValue().getSort());
      assertEquals(AFTER, options.getValue().getReturnDocument());

      verify(collection, times(1))
          .findOneAndUpdate(eq(filter), eq(setObject), any(FindOneAndUpdateOptions.class));
    }

    @Test
    void testAtomicUpdateWithoutUpdates() {
      assertThrows(
          IOException.class,
          () ->
              mongoCollection.update(
                  org.hypertrace.core.documentstore.query.Query.builder().build(),
                  emptyList(),
                  UpdateOptions.DEFAULT_UPDATE_OPTIONS));
    }
  }

  @Nested
  class BulkUpdateTest {
    private final org.hypertrace.core.documentstore.query.Query query =
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
    private final SubDocumentUpdate dateUpdate =
        SubDocumentUpdate.of("date", "2022-08-09T18:53:17Z");
    private final SubDocumentUpdate quantityUpdate = SubDocumentUpdate.of("quantity", 1000);
    private final SubDocumentUpdate propsUpdate =
        SubDocumentUpdate.of(
            "props", SubDocumentValue.of(new JSONDocument("{\"brand\": \"Dettol\"}")));
    private final BasicDBObject setObject =
        readBasicDBObject("atomic_read_and_update/set_object.json");
    private final BasicDBObject selections =
        readBasicDBObject("atomic_read_and_update/selection.json");
    private final BasicDBObject sort = readBasicDBObject("atomic_read_and_update/sort.json");
    private final BasicDBObject filter = readBasicDBObject("atomic_read_and_update/filter.json");
    private final BasicDBObject response =
        readBasicDBObject("atomic_read_and_update/response.json");

    BulkUpdateTest() throws IOException {}

    @SuppressWarnings("unchecked")
    @Test
    void testBulkUpdateWithFilter() throws IOException {
      final AggregateIterable<BasicDBObject> mockIterable = mock(AggregateIterable.class);
      final MongoCursor<BasicDBObject> mockCursor = mock(MongoCursor.class);

      when(mockClock.millis()).thenReturn(1660721309000L);
      when(collection.aggregate(anyList())).thenReturn(mockIterable);
      when(mockIterable.allowDiskUse(true)).thenReturn(mockIterable);
      when(mockIterable.cursor()).thenReturn(mockCursor);
      when(mockCursor.hasNext()).thenReturn(true).thenReturn(false);
      when(mockCursor.next()).thenReturn(response);

      final CloseableIterator<Document> result =
          mongoCollection.bulkUpdate(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate),
              UpdateOptions.DEFAULT_UPDATE_OPTIONS);
      final ArgumentCaptor<com.mongodb.client.model.UpdateOptions> updateOptionsArgumentCaptor =
          ArgumentCaptor.forClass(com.mongodb.client.model.UpdateOptions.class);
      com.mongodb.client.model.UpdateOptions mongoUpdateOption =
          new com.mongodb.client.model.UpdateOptions();
      mongoUpdateOption.upsert(false);
      assertTrue(result.hasNext());
      assertJsonEquals(
          readFileFromResource("atomic_read_and_update/response.json").orElseThrow(),
          result.next().toJson());

      verify(collection, times(1))
          .updateMany(eq(filter), eq(setObject), updateOptionsArgumentCaptor.capture());
      assertEquals(updateOptionsArgumentCaptor.getValue().toString(), mongoUpdateOption.toString());
    }

    @Test
    void testBulkUpdateWithFilter_getNone() throws IOException {
      when(mockClock.millis()).thenReturn(1660721309000L);

      final CloseableIterator<Document> result =
          mongoCollection.bulkUpdate(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate),
              UpdateOptions.builder().returnDocumentType(NONE).upsert(false).build());

      final ArgumentCaptor<com.mongodb.client.model.UpdateOptions> updateOptionsArgumentCaptor =
          ArgumentCaptor.forClass(com.mongodb.client.model.UpdateOptions.class);

      assertFalse(result.hasNext());
      com.mongodb.client.model.UpdateOptions mongoUpdateOption =
          new com.mongodb.client.model.UpdateOptions();
      mongoUpdateOption.upsert(false);
      verify(collection, times(1))
          .updateMany(eq(filter), eq(setObject), updateOptionsArgumentCaptor.capture());
      assertEquals(updateOptionsArgumentCaptor.getValue().toString(), mongoUpdateOption.toString());
    }

    @Test
    void testBulkUpdateWithoutUpdates() {
      assertThrows(
          IOException.class,
          () ->
              mongoCollection.bulkUpdate(
                  org.hypertrace.core.documentstore.query.Query.builder().build(),
                  emptyList(),
                  UpdateOptions.DEFAULT_UPDATE_OPTIONS));
    }
  }
}
