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
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Selection;
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
    verifyNoMoreInteractions(collection, iterable, cursor, aggIterable);
  }

  @Test
  public void testFindSimple() {
    Query query = Query.builder().build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject projection = new BasicDBObject();

    verify(collection).getNamespace();
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

    verify(collection).getNamespace();
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

    verify(collection).getNamespace();
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
            .addSort(IdentifierExpression.of("name"), SortOrder.ASC)
            .build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject sortQuery = BasicDBObject.parse("{ marks: -1, name: 1}");
    BasicDBObject projection = new BasicDBObject();

    verify(collection).getNamespace();
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
        Query.builder().setPagination(Pagination.builder().limit(10).offset(50).build()).build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject projection = new BasicDBObject();

    verify(collection).getNamespace();
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
            .addSort(IdentifierExpression.of("name"), SortOrder.ASC)
            .setPagination(Pagination.builder().offset(50).limit(10).build())
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

    verify(collection).getNamespace();
    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable).sort(sortQuery);
    verify(iterable).skip(50);
    verify(iterable).limit(10);
    verify(iterable).cursor();
  }

  @Test
  public void testFindAndAggregateWithDuplicateAlias() {
    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price"), "value"),
            SelectionSpec.of(IdentifierExpression.of("quantity"), "value"),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();
    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("item"),
                    NOT_IN,
                    ConstantExpression.ofStrings(List.of("Soap", "Bottle"))))
            .build();

    Query query = Query.builder().setSelection(selection).setFilter(filter).build();

    assertThrows(IllegalArgumentException.class, () -> executor.find(query));
    verify(collection, NOT_INVOKED).getNamespace();
    verify(collection, NOT_INVOKED).find(any(BasicDBObject.class));
    verify(iterable, NOT_INVOKED).projection(any(BasicDBObject.class));
    verify(iterable, NOT_INVOKED).sort(any(BasicDBObject.class));
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable, NOT_INVOKED).cursor();

    assertThrows(IllegalArgumentException.class, () -> executor.aggregate(query));
    verify(collection, NOT_INVOKED).aggregate(anyList());
    verify(aggIterable, NOT_INVOKED).cursor();
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
                    + "       \"$sum\": 1"
                    + "     }"
                    + "   }"
                    + "}"),
            BasicDBObject.parse("{" + "\"$project\": {" + "    \"total\": \"$total\"" + "}" + "}"));

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
                    + "       \"$sum\": 1"
                    + "     }"
                    + "   }"
                    + "}"),
            BasicDBObject.parse(
                "{"
                    + "\"$project\": "
                    + "   {"
                    + "     name: 1,"
                    + "     total: \"$total\""
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
            BasicDBObject.parse(
                "{" + "\"$project\": {" + "   \"topper\": \"$topper\"" + " }" + "}"));

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
            BasicDBObject.parse("{" + "\"$project\": {" + "   \"total\": \"$total\"" + " }" + "}"));

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
            BasicDBObject.parse("{" + "\"$project\": {" + "   \"total\": \"$total\"" + " }" + "}"),
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
                    + "\"$project\": {"
                    + "     \"averageHighScore\": \"$averageHighScore\""
                    + " }"
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
            .addAggregation(IdentifierExpression.of("student"))
            .setPagination(Pagination.builder().offset(0).limit(10).build())
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

  @Test
  public void testUnwindAndGroup() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("class"), LTE, ConstantExpression.of(10)))
            .addAggregation(IdentifierExpression.of("class.students.courses"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("class.students"), true))
            .addFromClause(
                UnnestExpression.of(IdentifierExpression.of("class.students.courses"), true))
            .build();

    List<BasicDBObject> pipeline =
        List.of(
            BasicDBObject.parse("{\"$match\": {\"class\": {\"$lte\": 10}}}"),
            BasicDBObject.parse(
                "{\"$unwind\": {\"path\": \"$class.students\", \"preserveNullAndEmptyArrays\": true}}"),
            BasicDBObject.parse(
                "{\"$unwind\": {\"path\": \"$class.students.courses\", \"preserveNullAndEmptyArrays\": true}}"),
            BasicDBObject.parse(
                "{\"$group\": {\"_id\": {\"class\\\\u002estudents\\\\u002ecourses\": \"$class.students.courses\"}}}"));

    testAggregation(query, pipeline);
  }

  private void testAggregation(Query query, List<BasicDBObject> pipeline) {
    executor.aggregate(query);
    verify(collection).getNamespace();
    verify(collection).aggregate(pipeline);
    verify(aggIterable).cursor();
  }
}
