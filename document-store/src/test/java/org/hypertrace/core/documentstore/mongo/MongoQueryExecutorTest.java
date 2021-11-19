package org.hypertrace.core.documentstore.mongo;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.SortingOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortingOrder.DESC;
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
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.SortingSpec;
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

  private MongoQueryExecutor executor;

  private static final VerificationMode NOT_INVOKED = times(0);

  @BeforeEach
  void setUp() {
    executor = new MongoQueryExecutor(collection);

    when(collection.find(any(BasicDBObject.class))).thenReturn(iterable);
    when(collection.aggregate(anyList())).thenReturn(aggIterable);

    when(iterable.projection(any(BasicDBObject.class))).thenReturn(iterable);
    when(iterable.skip(anyInt())).thenReturn(iterable);
    when(iterable.limit(anyInt())).thenReturn(iterable);
    when(iterable.sort(any(BasicDBObject.class))).thenReturn(iterable);

    when(iterable.cursor()).thenReturn(cursor);
    when(aggIterable.cursor()).thenReturn(cursor);
  }

  @AfterEach
  void tearDown() {
    verify(collection).getNamespace();
    verifyNoMoreInteractions(collection, iterable, cursor, aggIterable);
  }

  @Test
  public void testFindSimple() {
    Query query = Query.builder().build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject projection = new BasicDBObject();

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
            .addSelection(IdentifierExpression.of("id"))
            .addSelection(IdentifierExpression.of("fname"), "name")
            .build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject projection = BasicDBObject.parse("{id: 1, name: \"$fname\"}");

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
            .setFilter(
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

    executor.find(query);

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
    BasicDBObject projection = new BasicDBObject();

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
            .addSort(IdentifierExpression.of("marks"), DESC)
            .addSort(IdentifierExpression.of("name"), SortingOrder.ASC)
            .build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject sortQuery = BasicDBObject.parse("{ marks: -1, name: 1}");
    BasicDBObject projection = new BasicDBObject();

    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable).sort(sortQuery);
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithPagination() {
    Query query = Query.builder().setLimit(10).setOffset(50).build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject projection = new BasicDBObject();

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
            .addSelection(IdentifierExpression.of("id"))
            .addSelection(IdentifierExpression.of("fname"), "name")
            .addSort(IdentifierExpression.of("marks"), DESC)
            .addSort(IdentifierExpression.of("name"), SortingOrder.ASC)
            .setLimit(10)
            .setOffset(50)
            .setFilter(
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

    executor.find(query);

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
    BasicDBObject projection = BasicDBObject.parse("{id: 1, name: \"$fname\"}");
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
            .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
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
            BasicDBObject.parse("{" + "\"$project\": {" + "    \"total\": 1" + "}" + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithProjections() {
    Query query =
        Query.builder()
            .addSelections(
                List.of(
                    SelectionSpec.of(
                        AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total"),
                    SelectionSpec.of(IdentifierExpression.of("name"))))
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
            BasicDBObject.parse(
                "{"
                    + "\"$project\": "
                    + "   {"
                    + "     name: 1,"
                    + "     total: 1"
                    + "   }"
                    + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithMultiLevelGrouping() {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(MIN, IdentifierExpression.of("rank")), "topper")
            .addAggregations(
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
                    + "}"),
            BasicDBObject.parse("{" + "\"$project\": {" + "   \"topper\": 1" + " }" + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithFilter() {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(SUM, IdentifierExpression.of("marks")), "total")
            .setFilter(
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
                    + "}"),
            BasicDBObject.parse("{" + "\"$project\": {" + "   \"total\": 1" + " }" + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithGroupingFilter() {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(
                    SUM,
                    FunctionExpression.builder()
                        .operand(IdentifierExpression.of("price"))
                        .operator(MULTIPLY)
                        .operand(IdentifierExpression.of("quantity"))
                        .build()),
                "total")
            .addAggregation(IdentifierExpression.of("order"))
            .setAggregationFilter(
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
                    + "}"),
            BasicDBObject.parse("{" + "\"$project\": {" + "   \"total\": 1" + " }" + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithSorting() {
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(
                    AVG, AggregateExpression.of(MAX, IdentifierExpression.of("mark"))),
                "averageHighScore")
            .addAggregation(IdentifierExpression.of("section"))
            .addSorts(
                List.of(
                    SortingSpec.of(IdentifierExpression.of("averageHighScore"), DESC),
                    SortingSpec.of(IdentifierExpression.of("section"), ASC)))
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
                    + "}"),
            BasicDBObject.parse(
                "{" + "\"$project\": {" + "     \"averageHighScore\": 1" + " }" + "}"));

    testAggregation(query, pipeline);
  }

  @Test
  public void testAggregateWithPagination() {
    Query query =
        Query.builder()
            .addAggregation(IdentifierExpression.of("student"))
            .setLimit(10)
            .setOffset(0)
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

  @Test
  public void testGetDistinctCount() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("class"), LTE, ConstantExpression.of(10)))
            .addAggregation(IdentifierExpression.of("class"))
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("section")),
                "section_count")
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse(
                "{"
                    + "\"$match\": "
                    + "{"
                    + "   \"class\": {"
                    + "       \"$lte\": 10"
                    + "    }"
                    + "}"
                    + "}"),
            BasicDBObject.parse(
                "{"
                    + "\"$group\": "
                    + "   { "
                    + "     _id: {"
                    + "       class: \"$class\""
                    + "     },"
                    + "     section_count: {"
                    + "       \"$addToSet\": \"$section\""
                    + "     } "
                    + "   }"
                    + "}"),
            BasicDBObject.parse(
                "{"
                    + "\"$project\": {"
                    + "    section_count: {"
                    + "       \"$size\": \"$section_count\""
                    + "    }"
                    + "}"
                    + "}"));

    testAggregation(query, pipeline);
  }

  private void testAggregation(Query query, List<BasicDBObject> pipeline) {
    executor.aggregate(query);
    verify(collection).aggregate(pipeline);
    verify(aggIterable).cursor();
  }
}
