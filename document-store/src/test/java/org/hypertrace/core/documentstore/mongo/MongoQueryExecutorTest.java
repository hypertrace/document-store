package org.hypertrace.core.documentstore.mongo;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.TestUtil.readFileFromResource;
import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.and;
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
import static org.hypertrace.core.documentstore.model.options.DataFreshness.NEAR_REALTIME_FRESHNESS;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.REALTIME_FRESHNESS;
import static org.hypertrace.core.documentstore.model.options.DataFreshness.SYSTEM_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.ReadPreference;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.model.config.AggregatePipelineMode;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.mongo.query.MongoQueryExecutor;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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

  @Mock private ConnectionConfig connectionConfig;

  private MongoQueryExecutor executor;

  private static final VerificationMode NOT_INVOKED = times(0);
  private static final String TENANT_ID = "tenant-id";

  @BeforeEach
  void setUp() {
    when(connectionConfig.aggregationPipelineMode())
        .thenReturn(AggregatePipelineMode.SORT_OPTIMIZED_IF_POSSIBLE);
    executor = new MongoQueryExecutor(collection, connectionConfig);

    when(collection.find(any(BasicDBObject.class))).thenReturn(iterable);
    when(collection.withReadPreference(any(ReadPreference.class))).thenReturn(collection);
    when(collection.aggregate(anyList())).thenReturn(aggIterable);

    when(iterable.projection(any(BasicDBObject.class))).thenReturn(iterable);
    when(iterable.skip(anyInt())).thenReturn(iterable);
    when(iterable.limit(anyInt())).thenReturn(iterable);
    when(iterable.sort(any(BasicDBObject.class))).thenReturn(iterable);

    when(connectionConfig.queryTimeout()).thenReturn(Duration.ofMinutes(1));

    when(iterable.cursor()).thenReturn(cursor);
    when(aggIterable.allowDiskUse(true)).thenReturn(aggIterable);
    when(aggIterable.maxTime(Duration.ofMinutes(1).toMillis(), MILLISECONDS))
        .thenReturn(aggIterable);
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
  public void testFindWithSelection() throws IOException, URISyntaxException {
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("id"))
            .addSelection(IdentifierExpression.of("fname"), "name")
            .build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject projection = readExpectedBasicDBObject("mongo/projection/projection1.json");

    verify(collection).getNamespace();
    verify(collection).find(mongoQuery);
    verify(iterable).projection(projection);
    verify(iterable, NOT_INVOKED).sort(any());
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithFilter() throws IOException, URISyntaxException {
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

    BasicDBObject mongoQuery = readExpectedBasicDBObject("mongo/filter/filter1.json");
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
  public void testFindWithSorting() throws IOException, URISyntaxException {
    Query query =
        Query.builder()
            .addSort(IdentifierExpression.of("marks"), DESC)
            .addSort(IdentifierExpression.of("name"), SortOrder.ASC)
            .build();

    executor.find(query);

    BasicDBObject mongoQuery = new BasicDBObject();
    BasicDBObject sortQuery = readExpectedBasicDBObject("mongo/sort/sort1.json");
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
  public void testFindWithAllClauses() throws IOException, URISyntaxException {
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

    BasicDBObject mongoQuery = readExpectedBasicDBObject("mongo/filter/filter2.json");
    BasicDBObject projection = readExpectedBasicDBObject("mongo/projection/projection1.json");
    BasicDBObject sortQuery = readExpectedBasicDBObject("mongo/sort/sort1.json");

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
  public void testSimpleAggregate() throws IOException, URISyntaxException {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
            .build();
    testAggregation(query, "mongo/pipeline/simple.json");
  }

  @Test
  public void testFieldCount() throws IOException, URISyntaxException {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("path")), "total")
            .build();

    testAggregation(query, "mongo/pipeline/field_count.json");
  }

  @Test
  public void testAggregateWithProjections() throws IOException, URISyntaxException {
    Query query =
        Query.builder()
            .addSelections(
                List.of(
                    SelectionSpec.of(
                        AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total"),
                    SelectionSpec.of(IdentifierExpression.of("name"))))
            .build();

    testAggregation(query, "mongo/pipeline/with_projections.json");
  }

  @Test
  public void testAggregateWithMultiLevelGrouping() throws IOException, URISyntaxException {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(MIN, IdentifierExpression.of("rank")), "topper")
            .addAggregations(
                List.of(IdentifierExpression.of("name"), IdentifierExpression.of("class")))
            .build();

    testAggregation(query, "mongo/pipeline/multi_level_grouping.json");
  }

  @Test
  public void testAggregateWithFilter() throws IOException, URISyntaxException {
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(SUM, IdentifierExpression.of("marks")), "total")
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("section"),
                    IN,
                    ConstantExpression.ofStrings(List.of("A", "B", "C"))))
            .build();

    testAggregation(query, "mongo/pipeline/with_filter.json");
  }

  @Test
  public void testAggregateWithGroupingFilter() throws IOException, URISyntaxException {
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

    testAggregation(query, "mongo/pipeline/with_grouping_filter.json");
  }

  @Test
  public void testAggregateWithSorting() throws IOException, URISyntaxException {
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

    testAggregation(query, "mongo/pipeline/with_sorting.json");
  }

  @Test
  public void testAggregateWithPagination() throws IOException, URISyntaxException {
    Query query =
        Query.builder()
            .addAggregation(IdentifierExpression.of("student"))
            .setPagination(Pagination.builder().offset(0).limit(10).build())
            .build();

    testAggregation(query, "mongo/pipeline/with_pagination.json");
  }

  @Test
  public void testGetDistinctCount() throws IOException, URISyntaxException {
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

    testAggregation(query, "mongo/pipeline/distinct_count.json");
  }

  @Test
  public void testUnwindAndGroup() throws IOException, URISyntaxException {
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

    testAggregation(query, "mongo/pipeline/unwind_and_group.json");
  }

  @Test
  void testQueryWithFunctionalLhsInRelationalFilter() throws IOException, URISyntaxException {
    final Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .setFilter(
                RelationalExpression.of(
                    FunctionExpression.builder()
                        .operator(MULTIPLY)
                        .operand(IdentifierExpression.of("quantity"))
                        .operand(IdentifierExpression.of("price"))
                        .build(),
                    GT,
                    ConstantExpression.of(50)))
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    testAggregation(query, "mongo/pipeline/functional_lhs_in_relational_filter.json");
  }

  @Test
  public void testFindWithSingleKey() throws IOException, URISyntaxException {
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(KeyExpression.of(new SingleValueKey(TENANT_ID, "7")))
            .build();

    final BasicDBObject parsed = readExpectedBasicDBObject("mongo/filter/single_key_filter.json");
    executor.find(Query.builder().setFilter(filter).build());

    verify(collection).getNamespace();
    verify(collection).find(parsed);
    verify(iterable).projection(new BasicDBObject());
    verify(iterable, NOT_INVOKED).sort(any());
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithMultipleKeys() throws IOException, URISyntaxException {
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                and(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "30"))))
            .build();

    final BasicDBObject parsed = readExpectedBasicDBObject("mongo/filter/two_key_filter.json");
    executor.find(Query.builder().setFilter(filter).build());

    verify(collection).getNamespace();
    verify(collection).find(parsed);
    verify(iterable).projection(new BasicDBObject());
    verify(iterable, NOT_INVOKED).sort(any());
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testFindWithKeyAndRelationalFilter() throws IOException, URISyntaxException {
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                and(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    RelationalExpression.of(
                        IdentifierExpression.of("item"), NEQ, ConstantExpression.of("Comb"))))
            .build();
    final BasicDBObject parsed =
        readExpectedBasicDBObject("mongo/filter/key_and_relational_filter.json");
    executor.find(Query.builder().setFilter(filter).build());

    verify(collection).getNamespace();
    verify(collection).find(parsed);
    verify(iterable).projection(new BasicDBObject());
    verify(iterable, NOT_INVOKED).sort(any());
    verify(iterable, NOT_INVOKED).skip(anyInt());
    verify(iterable, NOT_INVOKED).limit(anyInt());
    verify(iterable).cursor();
  }

  @Test
  public void testOptimizeSorts_simpleAggregate() throws IOException, URISyntaxException {
    final Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    testAggregation(query, "mongo/pipeline/optimize_sorts_simple_aggregate.json");
  }

  @Test
  public void testOptimizeSorts_sortSpecNotInSelection() throws IOException, URISyntaxException {
    final Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSort(IdentifierExpression.of("item.description"), DESC)
            .build();

    testAggregation(query, "mongo/pipeline/optimize_sorts_sort_spec_not_in_selection.json");
  }

  @Test
  public void testOptimizeSorts_sortSpecNotInSelection_selectionWithAlias()
      throws IOException, URISyntaxException {
    final Query query =
        Query.builder()
            .addSelection(SelectionSpec.of(IdentifierExpression.of("item"), "item"))
            .addSort(IdentifierExpression.of("item.description"), DESC)
            .build();

    testAggregation(query, "mongo/pipeline/optimize_sorts_selection_with_alias.json");
  }

  @Test
  public void testOptimizeSorts_simpleSortWithFunctionAsSelection()
      throws IOException, URISyntaxException {
    final Query query =
        Query.builder()
            .addSelection(
                FunctionExpression.builder()
                    .operand(IdentifierExpression.of("price"))
                    .operator(MULTIPLY)
                    .operand(IdentifierExpression.of("quantity"))
                    .build(),
                "total")
            .addSort(IdentifierExpression.of("item.description"), DESC)
            .build();

    testAggregation(
        query, "mongo/pipeline/optimize_sorts_simple_sort_with_function_as_selection.json");
  }

  @Test
  public void testOptimizeSorts_simpleSortWithAggregationAsSelection()
      throws IOException, URISyntaxException {
    final Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
            .addSort(IdentifierExpression.of("item.description"), DESC)
            .build();

    testAggregation(
        query, "mongo/pipeline/optimize_sorts_simple_sort_with_aggregation_selection.json");
  }

  @Test
  public void testOptimizeSorts_sortSpecInSelection_selectionWithAlias()
      throws IOException, URISyntaxException {
    final Query query =
        Query.builder()
            .addSelection(SelectionSpec.of(IdentifierExpression.of("item.name"), "name"))
            .addSort(IdentifierExpression.of("name"), DESC)
            .build();

    testAggregation(
        query, "mongo/pipeline/optimize_sorts_sort_in_selection_selection_with_alias.json");
  }

  @SuppressWarnings("resource")
  @Nested
  class MongoQueryOptionsTest {
    @Test
    void testDefault() {
      final Query query =
          Query.builder()
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
              .build();
      executor.aggregate(query, QueryOptions.DEFAULT_QUERY_OPTIONS);

      // Fire query twice to test the cache behaviour
      executor.aggregate(query, QueryOptions.DEFAULT_QUERY_OPTIONS);

      testQueryOptions(ReadPreference.primary());
    }

    @Test
    void testConnectionLevelReadPreference() {
      final Query query =
          Query.builder()
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
              .build();

      when(connectionConfig.dataFreshness()).thenReturn(NEAR_REALTIME_FRESHNESS);
      executor.aggregate(query, QueryOptions.DEFAULT_QUERY_OPTIONS);

      // Fire query twice to test the cache behaviour
      executor.aggregate(query, QueryOptions.DEFAULT_QUERY_OPTIONS);

      testQueryOptions(ReadPreference.secondaryPreferred());
    }

    @Test
    void testConnectionLevelReadPreferenceWithNullValue() {
      final Query query =
          Query.builder()
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
              .build();

      when(connectionConfig.dataFreshness()).thenReturn(null);
      executor.aggregate(query, QueryOptions.DEFAULT_QUERY_OPTIONS);

      // Fire query twice to test the cache behaviour
      executor.aggregate(query, QueryOptions.DEFAULT_QUERY_OPTIONS);

      testQueryOptions(ReadPreference.primary());
    }

    @Test
    void testConnectionLevelReadPreferenceWithSystemDefaultValue() {
      final Query query =
          Query.builder()
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
              .build();

      when(connectionConfig.dataFreshness()).thenReturn(SYSTEM_DEFAULT);
      executor.aggregate(query, QueryOptions.DEFAULT_QUERY_OPTIONS);

      // Fire query twice to test the cache behaviour
      executor.aggregate(query, QueryOptions.DEFAULT_QUERY_OPTIONS);

      testQueryOptions(ReadPreference.primary());
    }

    @Test
    void testQueryLevelReadPreferenceOverride() {
      final Query query =
          Query.builder()
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
              .build();

      when(connectionConfig.dataFreshness()).thenReturn(NEAR_REALTIME_FRESHNESS);
      executor.aggregate(query, QueryOptions.builder().dataFreshness(REALTIME_FRESHNESS).build());

      // Fire query twice to test the cache behaviour
      executor.aggregate(query, QueryOptions.builder().dataFreshness(REALTIME_FRESHNESS).build());

      testQueryOptions(ReadPreference.primary());
    }

    @Test
    void testQueryLevelReadPreference() {
      final Query query =
          Query.builder()
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "total")
              .build();

      when(connectionConfig.dataFreshness()).thenReturn(SYSTEM_DEFAULT);
      executor.aggregate(
          query, QueryOptions.builder().dataFreshness(NEAR_REALTIME_FRESHNESS).build());

      // Fire query twice to test the cache behaviour
      executor.aggregate(
          query, QueryOptions.builder().dataFreshness(NEAR_REALTIME_FRESHNESS).build());

      testQueryOptions(ReadPreference.secondaryPreferred());
    }

    private void testQueryOptions(final ReadPreference readPreference) {
      verify(collection, times(2)).getNamespace();
      verify(collection, times(1)).withReadPreference(readPreference);
      verify(collection, times(2)).aggregate(anyList());
      verify(aggIterable, times(2)).allowDiskUse(true);
      verify(aggIterable, times(2)).cursor();
      verify(aggIterable, times(2)).maxTime(Duration.ofMinutes(1).toMillis(), MILLISECONDS);
    }
  }

  private void testAggregation(Query query, final String filePath)
      throws IOException, URISyntaxException {
    List<BasicDBObject> pipeline = readExpectedPipeline(filePath);

    executor.aggregate(query);
    verify(collection).getNamespace();
    verify(collection).withReadPreference(ReadPreference.primary());
    verify(collection).aggregate(pipeline);
    verify(aggIterable).allowDiskUse(true);
    verify(aggIterable).maxTime(Duration.ofMinutes(1).toMillis(), MILLISECONDS);
    verify(aggIterable).cursor();
  }

  @SuppressWarnings("Convert2Diamond")
  private List<BasicDBObject> readExpectedPipeline(final String filePath)
      throws IOException, URISyntaxException {
    final List<Map<String, Object>> fileContents =
        new ObjectMapper()
            .readValue(
                readFileFromResource(filePath), new TypeReference<List<Map<String, Object>>>() {});
    return fileContents.stream().map(BasicDBObject::new).collect(toUnmodifiableList());
  }

  private BasicDBObject readExpectedBasicDBObject(final String filePath)
      throws IOException, URISyntaxException {
    return BasicDBObject.parse(readFileFromResource(filePath));
  }
}
