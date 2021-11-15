package org.hypertrace.core.documentstore.mongo;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.SortingOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortingOrder.DESC;
import static org.hypertrace.core.documentstore.query.AllSelection.ALL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.query.PaginationDefinition;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SortingDefinition;
import org.hypertrace.core.documentstore.query.WhitelistedSelection;
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

  @Mock private AggregateIterable<BasicDBObject> aggIterable;

  @Mock private MongoCursor<BasicDBObject> cursor;

  private static final VerificationMode NOT_INVOKED = times(0);

  @BeforeEach
  void setUp() {
    when(collection.find(any(Bson.class))).thenReturn(iterable);
    when(collection.aggregate(anyList())).thenReturn(aggIterable);

    when(iterable.projection(any(Bson.class))).thenReturn(iterable);
    when(iterable.skip(anyInt())).thenReturn(iterable);
    when(iterable.limit(anyInt())).thenReturn(iterable);
    when(iterable.sort(any(Bson.class))).thenReturn(iterable);

    when(iterable.cursor()).thenReturn(cursor);
    when(aggIterable.cursor()).thenReturn(cursor);
  }

  @AfterEach
  void tearDown() {
    verifyNoMoreInteractions(collection, iterable, cursor, aggIterable);
  }

  @Test
  public void testFindSimple() {
    Query query = Query.builder().selection(ALL).build();

    MongoQueryExecutor.find(query, collection);

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

    MongoQueryExecutor.find(query, collection);

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

    MongoQueryExecutor.find(query, collection);

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
            .sortingDefinition(IdentifierExpression.of("marks"), DESC)
            .sortingDefinition(IdentifierExpression.of("name"), SortingOrder.ASC)
            .build();

    MongoQueryExecutor.find(query, collection);

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

    MongoQueryExecutor.find(query, collection);

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
            .sortingDefinition(IdentifierExpression.of("marks"), DESC)
            .sortingDefinition(IdentifierExpression.of("name"), SortingOrder.ASC)
            .paginationDefinition(PaginationDefinition.of(10, 50))
            .filter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("percentage"), GTE, ConstantExpression.of(90)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("class"), NEQ, ConstantExpression.of("XII")))
                    .build())
            .build();

    MongoQueryExecutor.find(query, collection);

    BasicDBObject mongoQuery =
        BasicDBObject.parse(
            "{"
                + "$and: ["
                + " {"
                + "   \"percentage\": { $gte: 90 }"
                + " },"
                + " {"
                + "   \"class\": { $ne: \"XII\" }"
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

  @Test
  public void testSimpleAggregate() {
    Query query =
        Query.builder()
            .selection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse(
                "{"
                    + "\"$group\": "
                    + "   { "
                    + "     _id: null, "
                    + "     total: {"
                    + "       \"$count\": 1"
                    + "     }"
                    + "   }"
                    + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithProjections() {
    Query query =
        Query.builder()
            .selections(
                List.of(
                    WhitelistedSelection.of(
                        AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total"),
                    WhitelistedSelection.of(IdentifierExpression.of("name"))))
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse(
                "{"
                    + "\"$group\": "
                    + "   { "
                    + "     _id: null, "
                    + "     total: {"
                    + "       \"$count\": 1"
                    + "     }"
                    + "   }"
                    + "}"),
            BasicDBObject.parse("{" + "\"$project\": " + "   {" + "     name: 1" + "   }" + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithMultiLevelGrouping() {
    Query query =
        Query.builder()
            .selection(AggregateExpression.of(MIN, IdentifierExpression.of("rank")), "topper")
            .aggregations(
                List.of(IdentifierExpression.of("name"), IdentifierExpression.of("class")))
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse(
                "{"
                    + "\"$group\": "
                    + "   { "
                    + "     _id: {"
                    + "        name: \"$name\","
                    + "        class: \"$class\""
                    + "     }, "
                    + "     topper: {"
                    + "       \"$min\": \"$rank\""
                    + "     }"
                    + "   }"
                    + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithFilter() {
    Query query =
        Query.builder()
            .selection(AggregateExpression.of(SUM, IdentifierExpression.of("marks")), "total")
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("section"),
                    IN,
                    ConstantExpression.ofStrings(List.of("A", "B", "C"))))
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse(
                "{"
                    + "\"$match\": "
                    + "   {"
                    + "      \"section\": {"
                    + "         \"$in\": [\"A\", \"B\", \"C\"]"
                    + "       }"
                    + "   }"
                    + "}"),
            BasicDBObject.parse(
                "{"
                    + "\"$group\": "
                    + "   { "
                    + "     _id: null, "
                    + "     total: {"
                    + "       \"$sum\": \"$marks\" "
                    + "     }"
                    + "   }"
                    + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithGroupingFilter() {
    Query query =
        Query.builder()
            .selection(
                AggregateExpression.of(
                    SUM,
                    FunctionExpression.builder()
                        .operand(IdentifierExpression.of("price"))
                        .operator(MULTIPLY)
                        .operand(IdentifierExpression.of("quantity"))
                        .build()),
                "total")
            .aggregation(IdentifierExpression.of("order"))
            .aggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("total"),
                    NOT_IN,
                    ConstantExpression.ofNumbers(List.of(100, 200, 500))))
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse(
                "{"
                    + "\"$group\": "
                    + "   { "
                    + "     _id: {"
                    + "        order: \"$order\""
                    + "     }, "
                    + "     total: {"
                    + "       \"$sum\": {"
                    + "         \"$multiply\": [ \"$price\", \"$quantity\" ]"
                    + "       }"
                    + "     }"
                    + "   }"
                    + "}"),
            BasicDBObject.parse(
                "{"
                    + "\"$match\":"
                    + "   {"
                    + "     total: { "
                    + "       $nin: [100, 200, 500] "
                    + "     }"
                    + "   }"
                    + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithSorting() {
    Query query =
        Query.builder()
            .selection(
                AggregateExpression.of(
                    AVG, AggregateExpression.of(MAX, IdentifierExpression.of("mark"))),
                "averageHighScore")
            .aggregation(IdentifierExpression.of("section"))
            .sortingDefinitions(
                List.of(
                    SortingDefinition.of(IdentifierExpression.of("averageHighScore"), DESC),
                    SortingDefinition.of(IdentifierExpression.of("section"), ASC)))
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse(
                "{"
                    + "\"$group\": "
                    + "   { "
                    + "     _id: {"
                    + "       section: \"$section\""
                    + "     }, "
                    + "     averageHighScore: {"
                    + "       \"$avg\": {"
                    + "         \"$max\": \"$mark\""
                    + "       }"
                    + "     }"
                    + "   }"
                    + "}"),
            BasicDBObject.parse(
                "{"
                    + "   \"$sort\": {"
                    + "       averageHighScore: -1,"
                    + "       section: 1"
                    + "   }"
                    + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithPagination() {
    Query query =
        Query.builder()
            .selection(ALL)
            .aggregation(IdentifierExpression.of("student"))
            .paginationDefinition(PaginationDefinition.of(10))
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse(
                "{"
                    + "\"$group\": "
                    + "   { "
                    + "     _id: {"
                    + "       student: \"$student\""
                    + "     } "
                    + "   }"
                    + "}"),
            BasicDBObject.parse("{" + "\"$skip\": 0" + "}"),
            BasicDBObject.parse("{" + "\"$limit\": 10" + "}"));

    testAggregation(query, pipeline);
  }

  private void testAggregation(Query query, List<BasicDBObject> pipeline) {
    MongoQueryExecutor.aggregate(query, collection);
    verify(collection).aggregate(pipeline);
    verify(aggIterable).cursor();
  }
}
