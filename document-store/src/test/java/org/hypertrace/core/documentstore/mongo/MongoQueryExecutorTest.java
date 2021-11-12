package org.hypertrace.core.documentstore.mongo;

import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.query.AllSelection.ALL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.io.IOException;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.query.PaginationDefinition;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.verification.VerificationMode;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MongoQueryExecutorTest {

  @Mock private com.mongodb.client.MongoCollection<BasicDBObject> collection;

  @Mock private FindIterable<BasicDBObject> iterable;

  @Mock private MongoCursor<BasicDBObject> cursor;

  private static final VerificationMode NOT_INVOKED = times(0);

  private static Document convertor(BasicDBObject object) {
    try {
      return new JSONDocument(object);
    } catch (IOException e) {
      return JSONDocument.errorDocument(e.getMessage());
    }
  }

  @BeforeEach
  void setUp() {
    when(collection.find(any(Bson.class))).thenReturn(iterable);

    when(iterable.projection(any(Bson.class))).thenReturn(iterable);
    when(iterable.skip(anyInt())).thenReturn(iterable);
    when(iterable.limit(anyInt())).thenReturn(iterable);
    when(iterable.sort(any(Bson.class))).thenReturn(iterable);

    when(iterable.cursor()).thenReturn(cursor);
  }

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(collection, iterable, cursor);
  }

  @Test
  public void testFindSimple() {
    Query query = Query.builder().selection(ALL).build();

    MongoQueryExecutor.find(query, collection, MongoQueryExecutorTest::convertor);

    BasicDBObject mongoQuery = new BasicDBObject();
    Bson projection = new BsonDocument();

    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable, NOT_INVOKED).sort(any());
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithSelection() {
    Query query =
        Query.builder()
            .selection(IdentifierExpression.of("id"))
            .selection(IdentifierExpression.of("fname"), "name")
            .build();

    MongoQueryExecutor.find(query, collection, MongoQueryExecutorTest::convertor);

    BasicDBObject mongoQuery = new BasicDBObject();
    Bson projection = BsonDocument.parse("{id: 1, fname: 1}");

    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable, NOT_INVOKED).sort(any());
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithFilter() {
    Query query =
        Query.builder()
            .selection(ALL)
            .filter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("percentage"), GT, ConstantExpression.of(90)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("class"), EQ, ConstantExpression.of("XII")))
                    .build())
            .build();

    MongoQueryExecutor.find(query, collection, MongoQueryExecutorTest::convertor);

    BasicDBObject mongoQuery =
        BasicDBObject.parse(
            "{"
                + "$and: ["
                + " {"
                + "   \"percentage\": { $gt: 90 }"
                + " },"
                + " {"
                + "   \"class\": \"XII\""
                + " }"
                + "]"
                + "}");
    Bson projection = new BsonDocument();

    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable, NOT_INVOKED).sort(any());
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithSorting() {
    Query query =
        Query.builder()
            .selection(ALL)
            .sortingDefinition(IdentifierExpression.of("marks"), SortingOrder.DESC)
            .sortingDefinition(IdentifierExpression.of("name"), SortingOrder.ASC)
            .build();

    MongoQueryExecutor.find(query, collection, MongoQueryExecutorTest::convertor);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject sortQuery = BasicDBObject.parse("{ marks: -1, name: 1}");
    Bson projection = new BsonDocument();

    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable).sort(sortQuery);
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithPagination() {
    Query query =
        Query.builder()
            .selection(ALL)
            .paginationDefinition(PaginationDefinition.of(10, 50))
            .build();

    MongoQueryExecutor.find(query, collection, MongoQueryExecutorTest::convertor);

    BasicDBObject mongoQuery = new BasicDBObject();
    Bson projection = new BsonDocument();

    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable, NOT_INVOKED).sort(any());
    verify(iterable).skip(50);
    verify(iterable).limit(10);
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithAllClauses() {
    Query query =
        Query.builder()
            .selection(IdentifierExpression.of("id"))
            .selection(IdentifierExpression.of("fname"), "name")
            .sortingDefinition(IdentifierExpression.of("marks"), SortingOrder.DESC)
            .sortingDefinition(IdentifierExpression.of("name"), SortingOrder.ASC)
            .paginationDefinition(PaginationDefinition.of(10, 50))
            .filter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("percentage"), GT, ConstantExpression.of(90)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("class"), EQ, ConstantExpression.of("XII")))
                    .build())
            .build();

    MongoQueryExecutor.find(query, collection, MongoQueryExecutorTest::convertor);

    BasicDBObject mongoQuery =
        BasicDBObject.parse(
            "{"
                + "$and: ["
                + " {"
                + "   \"percentage\": { $gt: 90 }"
                + " },"
                + " {"
                + "   \"class\": \"XII\""
                + " }"
                + "]"
                + "}");
    Bson projection = BsonDocument.parse("{id: 1, fname: 1}");
    BasicDBObject sortQuery = BasicDBObject.parse("{ marks: -1, name: 1}");

    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable).sort(sortQuery);
    verify(iterable).skip(50);
    verify(iterable).limit(10);
    verify(iterable).cursor();
  }
}
