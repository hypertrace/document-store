package org.hypertrace.core.documentstore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.and;
import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.or;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_ARRAY;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.BEFORE_UPDATE;
import static org.hypertrace.core.documentstore.utils.Utils.MONGO_STORE;
import static org.hypertrace.core.documentstore.utils.Utils.POSTGRES_STORE;
import static org.hypertrace.core.documentstore.utils.Utils.TENANT_ID;
import static org.hypertrace.core.documentstore.utils.Utils.assertDocsAndSizeEqual;
import static org.hypertrace.core.documentstore.utils.Utils.assertDocsAndSizeEqualWithoutOrder;
import static org.hypertrace.core.documentstore.utils.Utils.convertJsonToMap;
import static org.hypertrace.core.documentstore.utils.Utils.readFileFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.mongo.MongoDatastore;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Pagination;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.hypertrace.core.documentstore.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.Maps;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class DocStoreQueryV1Test {
  private static final String COLLECTION_NAME = "myTest";
  private static final String UPDATABLE_COLLECTION_NAME = "updatable_collection";

  private static Map<String, Datastore> datastoreMap;

  private static GenericContainer<?> mongo;
  private static GenericContainer<?> postgres;

  @BeforeAll
  public static void init() throws IOException {
    datastoreMap = Maps.newHashMap();
    mongo =
        new GenericContainer<>(DockerImageName.parse("mongo:4.4.0"))
            .withExposedPorts(27017)
            .waitingFor(Wait.forListeningPort());
    mongo.start();

    DatastoreProvider.register("MONGO", MongoDatastore.class);

    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.putIfAbsent("host", "localhost");
    mongoConfig.putIfAbsent("port", mongo.getMappedPort(27017).toString());
    Config config = ConfigFactory.parseMap(mongoConfig);

    Datastore mongoDatastore = DatastoreProvider.getDatastore("Mongo", config);
    System.out.println(mongoDatastore.listCollections());

    postgres =
        new GenericContainer<>(DockerImageName.parse("postgres:13.1"))
            .withEnv("POSTGRES_PASSWORD", "postgres")
            .withEnv("POSTGRES_USER", "postgres")
            .withExposedPorts(5432)
            .waitingFor(Wait.forListeningPort());
    postgres.start();

    String postgresConnectionUrl =
        String.format("jdbc:postgresql://localhost:%s/", postgres.getMappedPort(5432));
    DatastoreProvider.register("POSTGRES", PostgresDatastore.class);

    Map<String, String> postgresConfig = new HashMap<>();
    postgresConfig.putIfAbsent("url", postgresConnectionUrl);
    postgresConfig.putIfAbsent("user", "postgres");
    postgresConfig.putIfAbsent("password", "postgres");
    Datastore postgresDatastore =
        DatastoreProvider.getDatastore("Postgres", ConfigFactory.parseMap(postgresConfig));
    System.out.println(postgresDatastore.listCollections());

    datastoreMap.put(MONGO_STORE, mongoDatastore);
    datastoreMap.put(POSTGRES_STORE, postgresDatastore);

    createCollectionData("query/collection_data.json", COLLECTION_NAME);
  }

  private static void createCollectionData(final String resourcePath, final String collectionName)
      throws IOException {
    final Map<Key, Document> documents = Utils.buildDocumentsFromResource(resourcePath);
    datastoreMap.forEach(
        (k, v) -> {
          v.deleteCollection(collectionName);
          v.createCollection(collectionName, null);
          Collection collection = v.getCollection(collectionName);
          collection.bulkUpsert(documents);
        });
  }

  @AfterAll
  public static void shutdown() {
    mongo.stop();
    postgres.stop();
  }

  @MethodSource
  private static Stream<Arguments> databaseContextBoth() {
    return Stream.of(Arguments.of(MONGO_STORE), Arguments.of(POSTGRES_STORE));
  }

  @SuppressWarnings("unused")
  @MethodSource
  private static Stream<Arguments> databaseContextMongo() {
    return Stream.of(Arguments.of(MONGO_STORE));
  }

  @SuppressWarnings("unused")
  @MethodSource
  private static Stream<Arguments> databaseContextPostgres() {
    return Stream.of(Arguments.of(POSTGRES_STORE));
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindAll(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    Query query = Query.builder().build();
    Iterator<Document> resultDocs = collection.find(query);
    Utils.assertSizeEqual(resultDocs, "query/collection_data.json");

    testCountApi(dataStoreName, query, "query/collection_data.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testHasNext(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    Query query = Query.builder().build();
    Iterator<Document> resultDocs = collection.find(query);

    Utils.assertSizeEqual(resultDocs, "query/collection_data.json");
    // hasNext should not throw error even after cursor is closed
    assertFalse(resultDocs.hasNext());
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindSimple(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("quantity")),
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

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 5, "query/simple_filter_response.json");

    testCountApi(dataStoreName, query, "query/simple_filter_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithDuplicateSelections(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("quantity")),
            SelectionSpec.of(IdentifierExpression.of("quantity")),
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
    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 5, "query/simple_filter_response.json");

    testCountApi(dataStoreName, query, "query/simple_filter_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithDuplicateSortingAndPagination(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("quantity")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();

    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("item"),
                    IN,
                    ConstantExpression.ofStrings(List.of("Mirror", "Comb", "Shampoo", "Bottle"))))
            .build();

    Sort sort =
        Sort.builder()
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("quantity"), DESC))
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("item"), ASC))
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("quantity"), DESC))
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("item"), ASC))
            .build();

    Pagination pagination = Pagination.builder().offset(1).limit(3).build();

    Query query =
        Query.builder()
            .setSelection(selection)
            .setFilter(filter)
            .setSort(sort)
            .setPagination(pagination)
            .build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqual(
        dataStoreName, resultDocs, "query/filter_with_sorting_and_pagination_response.json", 3);

    testCountApi(dataStoreName, query, "query/filter_with_sorting_and_pagination_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithNestedFields(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("props.seller.name"), "seller"),
            SelectionSpec.of(IdentifierExpression.of("props.brand")),
            SelectionSpec.of(IdentifierExpression.of("props.seller.address.city")));

    Query query =
        Query.builder()
            .addSelections(selectionSpecs)
            .setFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"),
                            IN,
                            ConstantExpression.ofStrings(List.of("Mirror", "Comb", "Shampoo"))))
                    .operator(OR)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("props.seller.address.pincode"),
                            EQ,
                            ConstantExpression.of(700007)))
                    .build())
            .addSort(IdentifierExpression.of("props.brand"), ASC)
            .addSort(IdentifierExpression.of("item"), ASC)
            .addSort(IdentifierExpression.of("props.seller.address.city"), ASC)
            .build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqual(
        dataStoreName, resultDocs, "query/filter_on_nested_fields_response.json", 6);

    testCountApi(dataStoreName, query, "query/filter_on_nested_fields_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateEmpty(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query = Query.builder().build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    Utils.assertSizeEqual(resultDocs, "query/collection_data.json");
    testCountApi(dataStoreName, query, "query/collection_data.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateSimple(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(dataStoreName, resultDocs, 1, "query/count_response.json");
    testCountApi(dataStoreName, query, "query/count_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testOptionalFieldCount(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addSelection(
                AggregateExpression.of(COUNT, IdentifierExpression.of("props.seller.name")),
                "count")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 1, "query/optional_field_count_response.json");
    testCountApi(dataStoreName, query, "query/optional_field_count_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithDuplicateSelections(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(dataStoreName, resultDocs, 1, "query/count_response.json");
    testCountApi(dataStoreName, query, "query/count_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithFiltersAndOrdering(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
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
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .addSort(IdentifierExpression.of("total"), DESC)
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("total"), GTE, ConstantExpression.of(11)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("total"), LTE, ConstantExpression.of(99)))
                    .build())
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .setPagination(Pagination.builder().limit(10).offset(0).build())
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqual(dataStoreName, resultDocs, "query/sum_response.json", 2);
    testCountApi(dataStoreName, query, "query/sum_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithFiltersAndDuplicateOrderingAndDuplicateAggregations(
      String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
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
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .addSort(IdentifierExpression.of("total"), DESC)
            .addSort(IdentifierExpression.of("total"), DESC)
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("total"), GTE, ConstantExpression.of(11)))
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("total"), LTE, ConstantExpression.of(99)))
                    .build())
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .setPagination(Pagination.builder().limit(10).offset(0).build())
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqual(dataStoreName, resultDocs, "query/sum_response.json", 2);
    testCountApi(dataStoreName, query, "query/sum_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithNestedFields(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("props.seller.address.pincode"), "pincode.value")
            .addSelection(AggregateExpression.of(SUM, ConstantExpression.of(1)), "num_items.value")
            .addAggregation(IdentifierExpression.of("props.seller.address.pincode"))
            .addSort(IdentifierExpression.of("num_items.value"), DESC)
            .addSort(IdentifierExpression.of("pincode.value"), DESC)
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("num_items.value"), GT, ConstantExpression.of(1)))
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);

    if (dataStoreName.equals(POSTGRES_STORE)) {
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultDocs, 3, "query/pg_aggregate_on_nested_fields_response.json");
      testCountApi(dataStoreName, query, "query/pg_aggregate_on_nested_fields_response.json");
    } else {
      // NOTE that as part of this query, mongo impl returns a null field in the response. However,
      // in the rest of the other queries, it's not returning. So, we need to fix this inconsistency
      // in mongo impl. we should always return the null field or not. In Postgres, for
      // compatibility with the rest
      // of the mongo response, it is excluded in {@link PostgresResultIteratorWithMetaData}
      assertDocsAndSizeEqual(
          dataStoreName, resultDocs, "query/aggregate_on_nested_fields_response.json", 3);
      testCountApi(dataStoreName, query, "query/aggregate_on_nested_fields_response.json");
    }
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithoutAggregationAlias(String dataStoreName) {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(
                AggregateExpression.of(DISTINCT_ARRAY, IdentifierExpression.of("quantity")))
            .build();

    assertThrows(RuntimeException.class, () -> collection.aggregate(query));
    assertThrows(RuntimeException.class, () -> collection.count(query));
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithUnsupportedExpressionNesting(String dataStoreName) {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(
                AggregateExpression.of(DISTINCT_ARRAY, IdentifierExpression.of("quantity")),
                "quantities")
            .setAggregationFilter(
                RelationalExpression.of(
                    ConstantExpression.of(1),
                    GT,
                    FunctionExpression.builder()
                        .operator(LENGTH)
                        .operand(IdentifierExpression.of("quantities"))
                        .build()))
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    assertThrows(UnsupportedOperationException.class, () -> collection.aggregate(query));
    assertThrows(UnsupportedOperationException.class, () -> collection.count(query));
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithMultipleGroupingLevels(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(
                AggregateExpression.of(DISTINCT_ARRAY, IdentifierExpression.of("quantity")),
                "quantities")
            .addSelection(
                FunctionExpression.builder()
                    .operator(LENGTH)
                    .operand(IdentifierExpression.of("quantities"))
                    .build(),
                "num_quantities")
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("num_quantities"), EQ, ConstantExpression.of(1)))
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqual(
        dataStoreName, resultDocs, "query/multi_level_grouping_response.json", 2);
    testCountApi(dataStoreName, query, "query/multi_level_grouping_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithFunctionalLeftHandSideFilter(final String dataStoreName)
      throws IOException {
    final Datastore datastore = datastoreMap.get(dataStoreName);
    final Collection collection = datastore.getCollection(COLLECTION_NAME);

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

    final Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqual(
        dataStoreName, resultDocs, "query/test_functional_lhs_in_filter_response.json", 3);
    testCountApi(dataStoreName, query, "query/test_functional_lhs_in_filter_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithFunctionalLeftHandSideFilter(final String dataStoreName)
      throws IOException {
    final Datastore datastore = datastoreMap.get(dataStoreName);
    final Collection collection = datastore.getCollection(COLLECTION_NAME);

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

    final Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqual(
        dataStoreName, resultDocs, "query/test_functional_lhs_in_filter_response.json", 3);
    testCountApi(dataStoreName, query, "query/test_functional_lhs_in_filter_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryQ1AggregationFilterAlongWithNonAliasFields(String dataStoreName)
      throws IOException {
    Collection collection = getCollection(dataStoreName);
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("price"), GT, ConstantExpression.of(5)))
                    .build())
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 4, "query/test_aggr_alias_distinct_count_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryQ1AggregationFilterWithStringAlongWithNonAliasFields(String dataStoreName)
      throws IOException {
    Collection collection = getCollection(dataStoreName);
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"), EQ, ConstantExpression.of("Soap")))
                    .build())
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 2, "query/test_string_aggr_alias_distinct_count_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryQ1AggregationFilterWithStringInFilterAlongWithNonAliasFields(
      String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addAggregation(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("price"))
            .setAggregationFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"),
                            IN,
                            ConstantExpression.ofStrings(
                                List.of("Mirror", "Comb", "Shampoo", "Bottle"))))
                    .build())
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName,
        resultDocs,
        3,
        "query/test_string_in_filter_aggr_alias_distinct_count_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  void testQueryQ1DistinctCountAggregationWithOnlyFilter(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("price"), LTE, ConstantExpression.of(10)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("item"),
                            IN,
                            ConstantExpression.ofStrings(
                                List.of("Mirror", "Comb", "Shampoo", "Bottle"))))
                    .build())
            .build();

    try (CloseableIterator<Document> resultDocs = collection.aggregate(query)) {
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultDocs, 1, "query/test_aggr_only_with_fliter_response.json");
    }
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  void testQueryQ1DistinctCountAggregationWithMatchingSelectionAndGroupBy(String dataStoreName)
      throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), LTE, ConstantExpression.of(10)))
            .addAggregation(IdentifierExpression.of("item"))
            .build();
    try (CloseableIterator<Document> resultDocs = collection.aggregate(query)) {
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultDocs, 3, "query/test_aggr_with_match_selection_and_groupby.json");
    }
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1ForSimpleWhereClause(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    // query docs
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, iterator, 6, "query/simple_filter_quantity_neq_10.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1FilterWithNestedFiled(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    // query docs
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("props.seller.address.city"),
                            EQ,
                            ConstantExpression.of("Kolkata")))
                    .build())
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, iterator, 1, "query/test_nest_field_filter_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1ForFilterWithLogicalExpressionAndOr(String dataStoreName)
      throws IOException {
    Collection collection = getCollection(dataStoreName);

    // query docs
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                LogicalExpression.builder()
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("price"), EQ, ConstantExpression.of(10)))
                    .operator(OR)
                    .operand(
                        LogicalExpression.builder()
                            .operand(
                                RelationalExpression.of(
                                    IdentifierExpression.of("quantity"),
                                    GTE,
                                    ConstantExpression.of(5)))
                            .operator(AND)
                            .operand(
                                RelationalExpression.of(
                                    IdentifierExpression.of("quantity"),
                                    LTE,
                                    ConstantExpression.of(10)))
                            .build())
                    .build())
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 6, "query/filter_with_logical_and_or_operator.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1ForSelectionExpression(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    // query docs
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), EQ, ConstantExpression.of(10)))
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("props.brand"))
            .addSelection(IdentifierExpression.of("props.seller.name"))
            .addSelection(
                FunctionExpression.builder()
                    .operand(IdentifierExpression.of("price"))
                    .operator(MULTIPLY)
                    .operand(IdentifierExpression.of("quantity"))
                    .build(),
                "total")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 2, "query/test_selection_expression_result.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1FunctionalSelectionExpressionWithNestedFieldWithAlias(String dataStoreName)
      throws IOException {
    Collection collection = getCollection(dataStoreName);

    // query docs
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), EQ, ConstantExpression.of(10)))
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("props.brand"), "props_brand")
            .addSelection(IdentifierExpression.of("props.seller.name"), "props_seller_name")
            .addSelection(
                FunctionExpression.builder()
                    .operand(IdentifierExpression.of("price"))
                    .operator(MULTIPLY)
                    .operand(IdentifierExpression.of("quantity"))
                    .build(),
                "total")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName,
        resultDocs,
        2,
        "query/test_selection_expression_nested_fields_alias_result.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1AggregationExpression(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), GT, ConstantExpression.of(5)))
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(
                AggregateExpression.of(AVG, IdentifierExpression.of("quantity")), "qty_avg")
            .addSelection(
                AggregateExpression.of(COUNT, IdentifierExpression.of("quantity")), "qty_count")
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_distinct_count")
            .addSelection(
                AggregateExpression.of(SUM, IdentifierExpression.of("quantity")), "qty_sum")
            .addSelection(
                AggregateExpression.of(MIN, IdentifierExpression.of("quantity")), "qty_min")
            .addSelection(
                AggregateExpression.of(MAX, IdentifierExpression.of("quantity")), "qty_max")
            .addAggregation(IdentifierExpression.of("item"))
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 3, "query/test_aggregation_expression_result.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1AggregationFilter(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 4, "query/distinct_count_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1AggregationFilterWithWhereClause(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), LTE, ConstantExpression.of(7.5)))
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(10)))
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 2, "query/test_aggr_filter_and_where_filter_result.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testUnnestWithoutPreserveNullAndEmptyArrays(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), false))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), false))
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 11, "query/unwind_not_preserving_selection_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testUnnestWithoutPreserveNullAndEmptyArraysWithFilters(String dataStoreName)
      throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(false)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.city"),
                            EQ,
                            ConstantExpression.of("delhi")))
                    .build())
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), false))
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 3, "query/unwind_not_preserving_filter_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testUnnestWithPreserveNullAndEmptyArrays(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, 17, "query/unwind_preserving_selection_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testUnnestAndAggregate(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addAggregation(IdentifierExpression.of("sales.medium.type"))
            .addSelection(
                AggregateExpression.of(SUM, IdentifierExpression.of("sales.medium.volume")),
                "totalSales")
            // we don't want to consider entries where sales data is missing
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), false))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), false))
            .addSort(IdentifierExpression.of("totalSales"), DESC)
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqual(
        dataStoreName, iterator, "query/aggregate_on_nested_array_response.json", 3);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testUnnestAndAggregate_preserveEmptyTrue(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    // include all documents in the result irrespective of `sales` field
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, iterator, 1, "query/unwind_preserving_empty_array_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testUnnest(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .addSort(IdentifierExpression.of("item"), DESC)
            .addSort(IdentifierExpression.of("sales.city"), DESC)
            .addSort(IdentifierExpression.of("sales.medium.volume"), DESC)
            .addSort(IdentifierExpression.of("sales.medium.type"), DESC)
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqual(dataStoreName, iterator, "query/unwind_response.json", 17);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testUnnestAndAggregate_preserveEmptyFalse(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    // consider only those documents where sales field is missing
    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), false))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, iterator, 1, "query/unwind_not_preserving_empty_array_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFilterAndUnnest(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    RelationalExpression relationalExpression =
        RelationalExpression.of(
            IdentifierExpression.of("sales.city"), EQ, ConstantExpression.of("delhi"));

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium"))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(true)
                    .filterTypeExpression(relationalExpression)
                    .build())
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .addSort(IdentifierExpression.of("item"), DESC)
            .addSort(IdentifierExpression.of("sales.city"), DESC)
            .addSort(IdentifierExpression.of("sales.medium.volume"), DESC)
            .addSort(IdentifierExpression.of("sales.medium.type"), DESC)
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqual(dataStoreName, iterator, "query/unwind_filter_response.json", 7);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testQueryV1DistinctCountWithSortingSpecs(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(
                AggregateExpression.of(DISTINCT_COUNT, IdentifierExpression.of("quantity")),
                "qty_count")
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .setAggregationFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("qty_count"), LTE, ConstantExpression.of(1000)))
            .addSort(IdentifierExpression.of("qty_count"), DESC)
            .addSort(IdentifierExpression.of("item"), DESC)
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqual(dataStoreName, resultDocs, "query/distinct_count_response.json", 4);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithSortingAndPagination(String datastoreName) throws IOException {
    Collection collection = getCollection(datastoreName);

    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("price")),
            SelectionSpec.of(IdentifierExpression.of("quantity")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();

    org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("item"),
                    IN,
                    ConstantExpression.ofStrings(List.of("Mirror", "Comb", "Shampoo", "Bottle"))))
            .build();

    Sort sort =
        Sort.builder()
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("quantity"), DESC))
            .sortingSpec(SortingSpec.of(IdentifierExpression.of("item"), ASC))
            .build();

    Pagination pagination = Pagination.builder().offset(1).limit(3).build();

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .setSelection(selection)
            .setFilter(filter)
            .setSort(sort)
            .setPagination(pagination)
            .build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqual(
        datastoreName, resultDocs, "query/filter_with_sorting_and_pagination_response.json", 3);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithSingleKey(final String datastoreName) throws IOException {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(KeyExpression.of(new SingleValueKey(TENANT_ID, "7")))
            .build();

    final Iterator<Document> resultDocs =
        collection.find(Query.builder().setFilter(filter).build());
    assertDocsAndSizeEqual(datastoreName, resultDocs, "query/key_filter_response.json", 1);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithDuplicateKeys(final String datastoreName) throws IOException {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                and(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7"))))
            .build();

    final Iterator<Document> resultDocs =
        collection.find(Query.builder().setFilter(filter).build());
    assertDocsAndSizeEqual(datastoreName, resultDocs, "query/key_filter_response.json", 1);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithConflictingKeys(final String datastoreName) {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                and(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "8"))))
            .build();

    final Iterator<Document> resultDocs =
        collection.find(Query.builder().setFilter(filter).build());
    assertFalse(resultDocs.hasNext());
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithMultipleKeys(final String datastoreName) throws IOException {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                or(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "3"))))
            .build();

    final Iterator<Document> resultDocs =
        collection.find(Query.builder().setFilter(filter).build());
    assertDocsAndSizeEqualWithoutOrder(
        datastoreName, resultDocs, 2, "query/key_filter_multiple_response.json");
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithNonExistingKeys(final String datastoreName) {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(KeyExpression.of(new SingleValueKey(TENANT_ID, "30")))
            .build();

    final Iterator<Document> resultDocs =
        collection.find(Query.builder().setFilter(filter).build());
    assertFalse(resultDocs.hasNext());
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithKeyAndMatchingRelationalFilter(final String datastoreName)
      throws IOException {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                and(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    RelationalExpression.of(
                        IdentifierExpression.of("item"), NEQ, ConstantExpression.of("Soap"))))
            .build();

    final Iterator<Document> resultDocs =
        collection.find(Query.builder().setFilter(filter).build());
    assertDocsAndSizeEqual(datastoreName, resultDocs, "query/key_filter_response.json", 1);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithKeyAndNonMatchingRelationalFilter(final String datastoreName) {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                and(
                    KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                    RelationalExpression.of(
                        IdentifierExpression.of("item"), NEQ, ConstantExpression.of("Comb"))))
            .build();

    final Iterator<Document> resultDocs =
        collection.find(Query.builder().setFilter(filter).build());
    assertFalse(resultDocs.hasNext());
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testFindWithMultipleKeysAndPartiallyMatchingRelationalFilter(
      final String datastoreName) throws IOException {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(
                and(
                    or(
                        KeyExpression.of(new SingleValueKey(TENANT_ID, "7")),
                        KeyExpression.of(new SingleValueKey(TENANT_ID, "3"))),
                    RelationalExpression.of(
                        IdentifierExpression.of("item"), EQ, ConstantExpression.of("Comb"))))
            .build();

    final Iterator<Document> resultDocs =
        collection.find(Query.builder().setFilter(filter).build());
    assertDocsAndSizeEqual(datastoreName, resultDocs, "query/key_filter_response.json", 1);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAggregateWithSingleKey(final String datastoreName) throws IOException {
    final Collection collection = getCollection(datastoreName);
    final org.hypertrace.core.documentstore.query.Filter filter =
        org.hypertrace.core.documentstore.query.Filter.builder()
            .expression(KeyExpression.of(new SingleValueKey(TENANT_ID, "7")))
            .build();

    final Iterator<Document> resultDocs =
        collection.aggregate(Query.builder().setFilter(filter).build());
    assertDocsAndSizeEqual(datastoreName, resultDocs, "query/key_filter_response.json", 1);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAtomicUpdateWithFilter(final String datastoreName)
      throws IOException, ExecutionException, InterruptedException {
    final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
    createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

    final Query query =
        Query.builder()
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

    final Random random = new Random();
    final Callable<Optional<Document>> callable =
        () -> {
          MILLISECONDS.sleep(random.nextInt(1000));
          return collection.update(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate),
              UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());
        };

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final Future<Optional<Document>> future1 = executor.submit(callable);
    final Future<Optional<Document>> future2 = executor.submit(callable);

    final Optional<Document> doc1Optional = future1.get();
    final Optional<Document> doc2Optional = future2.get();

    assertTrue(doc1Optional.isPresent());
    assertTrue(doc2Optional.isPresent());

    final Document document1 = doc1Optional.get();
    final Document document2 = doc2Optional.get();

    assertNotEquals(document1, document2);
    assertDocsAndSizeEqualWithoutOrder(
        datastoreName,
        List.of(document1, document2).iterator(),
        2,
        "query/atomic_update_response.json");
    assertDocsAndSizeEqual(
        datastoreName,
        collection.find(
            Query.builder()
                .addSelection(IdentifierExpression.of("item"))
                .addSelection(IdentifierExpression.of("price"))
                .addSelection(IdentifierExpression.of("quantity"))
                .addSelection(IdentifierExpression.of("date"))
                .addSelection(IdentifierExpression.of("props.brand"), "brand")
                .addSort(IdentifierExpression.of("_id"), ASC)
                .build()),
        "query/updatable_collection_data_after_atomic_update.json",
        9);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAtomicUpdateWithFilterAndGetNewDocument(final String datastoreName)
      throws IOException, ExecutionException, InterruptedException {
    final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
    createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

    final Query query =
        Query.builder()
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
    final SubDocumentUpdate propsUpdate = SubDocumentUpdate.of("props.brand", "Dettol");
    final SubDocumentUpdate addProperty =
        SubDocumentUpdate.of(
            "props.new_property.deep.nested.value",
            SubDocumentValue.of(new JSONDocument("{\"json\": \"new_value\"}")));

    final Random random = new Random();
    final Callable<Optional<Document>> callable =
        () -> {
          MILLISECONDS.sleep(random.nextInt(1000));
          return collection.update(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate, addProperty),
              UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
        };

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final Future<Optional<Document>> future1 = executor.submit(callable);
    final Future<Optional<Document>> future2 = executor.submit(callable);

    final Optional<Document> doc1Optional = future1.get();
    final Optional<Document> doc2Optional = future2.get();

    assertTrue(doc1Optional.isPresent());
    assertTrue(doc2Optional.isPresent());

    final Document document1 = doc1Optional.get();
    final Document document2 = doc2Optional.get();

    assertNotEquals(document1, document2);
    assertDocsAndSizeEqualWithoutOrder(
        datastoreName,
        List.of(document1, document2).iterator(),
        2,
        "query/atomic_update_response_get_new_document.json");
    assertDocsAndSizeEqual(
        datastoreName,
        collection.find(
            Query.builder()
                .addSelection(IdentifierExpression.of("item"))
                .addSelection(IdentifierExpression.of("price"))
                .addSelection(IdentifierExpression.of("quantity"))
                .addSelection(IdentifierExpression.of("date"))
                .addSelection(IdentifierExpression.of("props"))
                .addSort(IdentifierExpression.of("_id"), ASC)
                .build()),
        "query/updatable_collection_data_after_atomic_update_selecting_all_props.json",
        9);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAtomicUpdateSameDocumentWithFilter(final String datastoreName)
      throws IOException, ExecutionException, InterruptedException {
    final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
    createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

    final Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("item"), EQ, ConstantExpression.of("Soap")))
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

    final Random random = new Random();
    final Callable<Optional<Document>> callable =
        () -> {
          MILLISECONDS.sleep(random.nextInt(1000));
          return collection.update(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate),
              UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());
        };

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final Future<Optional<Document>> future1 = executor.submit(callable);
    final Future<Optional<Document>> future2 = executor.submit(callable);

    final Optional<Document> doc1Optional = future1.get();
    final Optional<Document> doc2Optional = future2.get();

    assertTrue(doc1Optional.isPresent());
    assertTrue(doc2Optional.isPresent());

    final Document document1 = doc1Optional.get();
    final Document document2 = doc2Optional.get();

    assertNotEquals(document1, document2);
    assertDocsAndSizeEqualWithoutOrder(
        datastoreName,
        List.of(document1, document2).iterator(),
        2,
        "query/atomic_update_same_document_response.json");
    assertDocsAndSizeEqual(
        datastoreName,
        collection.find(
            Query.builder()
                .addSelection(IdentifierExpression.of("item"))
                .addSelection(IdentifierExpression.of("price"))
                .addSelection(IdentifierExpression.of("quantity"))
                .addSelection(IdentifierExpression.of("date"))
                .addSelection(IdentifierExpression.of("props.brand"), "brand")
                .addSort(IdentifierExpression.of("_id"), ASC)
                .build()),
        "query/updatable_collection_data_after_atomic_update_same_document.json",
        9);
  }

  @ParameterizedTest
  @MethodSource("databaseContextBoth")
  public void testAtomicUpdateDocumentWithoutSelections(final String datastoreName)
      throws IOException, ExecutionException, InterruptedException {
    final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
    createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

    final Query query =
        Query.builder()
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
            .build();

    final SubDocumentUpdate dateUpdate = SubDocumentUpdate.of("date", "2022-08-09T18:53:17Z");

    final Random random = new Random();
    final Callable<Optional<Document>> callable =
        () -> {
          MILLISECONDS.sleep(random.nextInt(1000));
          return collection.update(
              query,
              List.of(dateUpdate),
              UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());
        };

    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final Future<Optional<Document>> future1 = executor.submit(callable);
    final Future<Optional<Document>> future2 = executor.submit(callable);

    final Optional<Document> doc1Optional = future1.get();
    final Optional<Document> doc2Optional = future2.get();

    assertTrue(doc1Optional.isPresent());
    assertTrue(doc2Optional.isPresent());

    final Document document1 = doc1Optional.get();
    final Document document2 = doc2Optional.get();

    assertNotEquals(document1, document2);
    assertDocsAndSizeEqualWithoutOrder(
        datastoreName,
        collection.find(Query.builder().build()),
        9,
        "query/updatable_collection_data_without_selection.json");
  }

  private static Collection getCollection(final String dataStoreName) {
    return getCollection(dataStoreName, COLLECTION_NAME);
  }

  private static Collection getCollection(final String dataStoreName, final String collectionName) {
    final Datastore datastore = datastoreMap.get(dataStoreName);
    return datastore.getCollection(collectionName);
  }

  private static void testCountApi(
      final String dataStoreName, final Query query, final String filePath) throws IOException {
    Collection collection = getCollection(dataStoreName);
    final long actualSize = collection.count(query);
    final String fileContent = readFileFromResource(filePath).orElseThrow();
    final long expectedSize = convertJsonToMap(fileContent).size();
    assertEquals(expectedSize, actualSize);
  }
}
