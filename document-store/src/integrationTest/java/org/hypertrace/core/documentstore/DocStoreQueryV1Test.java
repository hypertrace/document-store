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
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NOT_IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.STARTS_WITH;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.BEFORE_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.ADD;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.APPEND_TO_LIST;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.REMOVE_ALL_FROM_LIST;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.SET;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.UNSET;
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
import org.hypertrace.core.documentstore.query.Query.QueryBuilder;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.hypertrace.core.documentstore.utils.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
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

  private static class AllProvider implements ArgumentsProvider {
    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(MONGO_STORE), Arguments.of(POSTGRES_STORE));
    }
  }

  private static class MongoProvider implements ArgumentsProvider {
    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(MONGO_STORE));
    }
  }

  private static class PostgresProvider implements ArgumentsProvider {
    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(Arguments.of(POSTGRES_STORE));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testFindAll(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    Query query = Query.builder().build();
    Iterator<Document> resultDocs = collection.find(query);
    Utils.assertSizeEqual(resultDocs, "query/collection_data.json");

    testCountApi(dataStoreName, query, "query/collection_data.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testHasNext(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    Query query = Query.builder().build();
    Iterator<Document> resultDocs = collection.find(query);

    Utils.assertSizeEqual(resultDocs, "query/collection_data.json");
    // hasNext should not throw error even after cursor is closed
    assertFalse(resultDocs.hasNext());
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/simple_filter_response.json", 5);

    testCountApi(dataStoreName, query, "query/simple_filter_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/simple_filter_response.json", 5);

    testCountApi(dataStoreName, query, "query/simple_filter_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(PostgresProvider.class)
  void testAggregateWithId(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    String documentString =
        "{\"id\":\"e2e7f827-7ea5-5a5f-b547-d737965e4e58\","
            + "\"_id\":\"e2e7f827-7ea5-5a5f-b547-d737965e4e58\","
            + "\"type\":\"VULNERABILITY\","
            + "\"tenantId\":\""
            + TENANT_ID
            + "\","
            + "\"attributes\":"
            + "{\"name\":\"X-Content-Type-Options without nosniff\","
            + "\"type\":\"VULNERABILITY_TYPE_MISSING_NOSNIFF_IN_CONTENT_TYPE_OPTIONS_HEADER\","
            + "\"status\":\"OPEN\","
            + "\"severity\":\"HIGH\","
            + "\"entity_id\":\"79d2ffc4-38a6-376f-a57f-89893f0acb5b\","
            + "\"service_id\":\"8d64ccfb-ad07-3a3c-bc32-740f1c794b7d\","
            + "\"entity_name\":\"POST/login\","
            + "\"entity_type\":\"API\","
            + "\"environment\":\"cluster001\","
            + "\"is_external\":true,\"service_name\":"
            + "\"nginx-traceshop\","
            + "\"detection_timestamp\":1663312992746,"
            + "\"status_update_timestamp\":1663312992746},"
            + "\"identifyingAttributes\":{"
            + "\"name\":\"X-Content-Type-Options without nosniff\","
            + "\"entity_id\":\"79d2ffc4-38a6-376f-a57f-89893f0acb5b\"}}";
    Document document = new JSONDocument(documentString);
    SingleValueKey key = new SingleValueKey(TENANT_ID, "e2e7f827-7ea5-5a5f-b547-d737965e4e58");
    collection.upsert(key, document);

    // Search nested field in the document
    QueryBuilder queryBuilder = org.hypertrace.core.documentstore.query.Query.builder();
    queryBuilder.addSelection(IdentifierExpression.of("id"));
    queryBuilder.addSelection(
        SelectionSpec.of(IdentifierExpression.of("attributes.status"), "STATUS"));
    queryBuilder.addSelection(
        SelectionSpec.of(IdentifierExpression.of("attributes.entity_id"), "ENTITY_ID"));
    queryBuilder.setFilter(
        LogicalExpression.builder()
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("tenantId"), EQ, ConstantExpression.of(TENANT_ID)))
            .operator(AND)
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("type"), EQ, ConstantExpression.of("VULNERABILITY")))
            .build());
    Iterator<Document> results = collection.aggregate(queryBuilder.build());
    assertDocsAndSizeEqual(dataStoreName, results, "query/aggregate_with_id.json", 1);

    // delete the document created for this test
    collection.delete(key);
  }

  @ParameterizedTest
  @ArgumentsSource(PostgresProvider.class)
  void testAggregateWithIdAlias(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);
    String documentString =
        "{\"id\":\"e2e7f827-7ea5-5a5f-b547-d737965e4e58\","
            + "\"_id\":\"e2e7f827-7ea5-5a5f-b547-d737965e4e58\","
            + "\"type\":\"VULNERABILITY\","
            + "\"tenantId\":\""
            + TENANT_ID
            + "\","
            + "\"attributes\":"
            + "{\"name\":\"X-Content-Type-Options without nosniff\","
            + "\"type\":\"VULNERABILITY_TYPE_MISSING_NOSNIFF_IN_CONTENT_TYPE_OPTIONS_HEADER\","
            + "\"status\":\"OPEN\","
            + "\"severity\":\"HIGH\","
            + "\"entity_id\":\"79d2ffc4-38a6-376f-a57f-89893f0acb5b\","
            + "\"service_id\":\"8d64ccfb-ad07-3a3c-bc32-740f1c794b7d\","
            + "\"entity_name\":\"POST/login\","
            + "\"entity_type\":\"API\","
            + "\"environment\":\"cluster001\","
            + "\"is_external\":true,\"service_name\":"
            + "\"nginx-traceshop\","
            + "\"detection_timestamp\":1663312992746,"
            + "\"status_update_timestamp\":1663312992746},"
            + "\"identifyingAttributes\":{"
            + "\"name\":\"X-Content-Type-Options without nosniff\","
            + "\"entity_id\":\"79d2ffc4-38a6-376f-a57f-89893f0acb5b\"}}";
    Document document = new JSONDocument(documentString);
    SingleValueKey key = new SingleValueKey(TENANT_ID, "e2e7f827-7ea5-5a5f-b547-d737965e4e58");
    collection.upsert(key, document);

    // Search nested field in the document
    QueryBuilder queryBuilder = org.hypertrace.core.documentstore.query.Query.builder();
    queryBuilder.addSelection(SelectionSpec.of(IdentifierExpression.of("id"), "ID"));
    queryBuilder.addSelection(
        SelectionSpec.of(IdentifierExpression.of("attributes.status"), "STATUS"));
    queryBuilder.addSelection(
        SelectionSpec.of(IdentifierExpression.of("attributes.entity_id"), "ENTITY_ID"));
    queryBuilder.setFilter(
        LogicalExpression.builder()
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("tenantId"), EQ, ConstantExpression.of(TENANT_ID)))
            .operator(AND)
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("type"), EQ, ConstantExpression.of("VULNERABILITY")))
            .build());
    Iterator<Document> results = collection.aggregate(queryBuilder.build());
    assertDocsAndSizeEqual(dataStoreName, results, "query/aggregate_with_caps_id.json", 1);

    // delete the document created for this test
    collection.delete(key);
  }

  @ParameterizedTest
  @ArgumentsSource(PostgresProvider.class)
  void testAggregateWithTestIdAlias(String dataStoreName) throws IOException {
    Datastore datastore = datastoreMap.get(dataStoreName);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    String documentString =
        "{\"id\":\"e2e7f827-7ea5-5a5f-b547-d737965e4e58\","
            + "\"_id\":\"e2e7f827-7ea5-5a5f-b547-d737965e4e58\","
            + "\"type\":\"VULNERABILITY\","
            + "\"tenantId\":\""
            + TENANT_ID
            + "\","
            + "\"attributes\":"
            + "{\"name\":\"X-Content-Type-Options without nosniff\","
            + "\"type\":\"VULNERABILITY_TYPE_MISSING_NOSNIFF_IN_CONTENT_TYPE_OPTIONS_HEADER\","
            + "\"status\":\"OPEN\","
            + "\"severity\":\"HIGH\","
            + "\"entity_id\":\"79d2ffc4-38a6-376f-a57f-89893f0acb5b\","
            + "\"service_id\":\"8d64ccfb-ad07-3a3c-bc32-740f1c794b7d\","
            + "\"entity_name\":\"POST/login\","
            + "\"entity_type\":\"API\","
            + "\"environment\":\"cluster001\","
            + "\"is_external\":true,\"service_name\":"
            + "\"nginx-traceshop\","
            + "\"detection_timestamp\":1663312992746,"
            + "\"status_update_timestamp\":1663312992746},"
            + "\"identifyingAttributes\":{"
            + "\"name\":\"X-Content-Type-Options without nosniff\","
            + "\"entity_id\":\"79d2ffc4-38a6-376f-a57f-89893f0acb5b\"}}";
    Document document = new JSONDocument(documentString);
    SingleValueKey key = new SingleValueKey(TENANT_ID, "e2e7f827-7ea5-5a5f-b547-d737965e4e58");
    collection.upsert(key, document);

    // Search nested field in the document
    QueryBuilder queryBuilder = org.hypertrace.core.documentstore.query.Query.builder();
    queryBuilder.addSelection(SelectionSpec.of(IdentifierExpression.of("id"), "TEST_ID"));
    queryBuilder.addSelection(
        SelectionSpec.of(IdentifierExpression.of("attributes.status"), "STATUS"));
    queryBuilder.addSelection(
        SelectionSpec.of(IdentifierExpression.of("attributes.entity_id"), "ENTITY_ID"));
    queryBuilder.setFilter(
        LogicalExpression.builder()
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("tenantId"), EQ, ConstantExpression.of(TENANT_ID)))
            .operator(AND)
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("type"), EQ, ConstantExpression.of("VULNERABILITY")))
            .build());
    Iterator<Document> results = collection.aggregate(queryBuilder.build());
    assertDocsAndSizeEqual(dataStoreName, results, "query/aggregate_with_test_id.json", 1);

    // delete the document created for this test
    collection.delete(key);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testAggregateEmpty(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query = Query.builder().build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    Utils.assertSizeEqual(resultDocs, "query/collection_data.json");
    testCountApi(dataStoreName, query, "query/collection_data.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testAggregateSimple(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(dataStoreName, resultDocs, "query/count_response.json", 1);
    testCountApi(dataStoreName, query, "query/count_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/optional_field_count_response.json", 1);
    testCountApi(dataStoreName, query, "query/optional_field_count_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testAggregateWithDuplicateSelections(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);
    Query query =
        Query.builder()
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
            .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(dataStoreName, resultDocs, "query/count_response.json", 1);
    testCountApi(dataStoreName, query, "query/count_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
          dataStoreName, resultDocs, "query/pg_aggregate_on_nested_fields_response.json", 3);
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
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/test_aggr_alias_distinct_count_response.json", 4);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/test_string_aggr_alias_distinct_count_response.json", 2);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        "query/test_string_in_filter_aggr_alias_distinct_count_response.json",
        3);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
          dataStoreName, resultDocs, "query/test_aggr_only_with_fliter_response.json", 1);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
          dataStoreName, resultDocs, "query/test_aggr_with_match_selection_and_groupby.json", 3);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, iterator, "query/simple_filter_quantity_neq_10.json", 6);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, iterator, "query/test_nest_field_filter_response.json", 1);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/filter_with_logical_and_or_operator.json", 6);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/test_selection_expression_result.json", 2);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        "query/test_selection_expression_nested_fields_alias_result.json",
        2);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/test_aggregation_expression_result.json", 3);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/distinct_count_response.json", 4);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/test_aggr_filter_and_where_filter_result.json", 2);
  }

  @Nested
  class StartsWithOperatorTest {
    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    public void testWithUnnestingAndRegularFilters(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName);

      final org.hypertrace.core.documentstore.query.Query query =
          org.hypertrace.core.documentstore.query.Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("price"))
              .addSelection(IdentifierExpression.of("sales.city"))
              .addSelection(IdentifierExpression.of("sales.medium.type"))
              .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), false))
              .addFromClause(
                  UnnestExpression.builder()
                      .filterTypeExpression(
                          RelationalExpression.of(
                              IdentifierExpression.of("sales.medium.type"),
                              STARTS_WITH,
                              ConstantExpression.of("distribution")))
                      .identifierExpression(IdentifierExpression.of("sales.medium"))
                      .preserveNullAndEmptyArrays(false)
                      .build())
              .setFilter(
                  Filter.builder()
                      .expression(
                          RelationalExpression.of(
                              IdentifierExpression.of("item"),
                              STARTS_WITH,
                              ConstantExpression.of("S")))
                      .build())
              .build();

      final Iterator<Document> resultDocs = collection.aggregate(query);
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, resultDocs, "query/starts_with_filter_response.json", 4);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    public void testRequirementForUsingIndex_ShouldBeCaseSensitive(final String datastoreName)
        throws IOException {
      final Collection collection = getCollection(datastoreName);

      final org.hypertrace.core.documentstore.query.Query query =
          org.hypertrace.core.documentstore.query.Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("price"))
              .addSelection(IdentifierExpression.of("sales.city"))
              .addSelection(IdentifierExpression.of("sales.medium.type"))
              .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), false))
              .addFromClause(
                  UnnestExpression.builder()
                      .filterTypeExpression(
                          RelationalExpression.of(
                              IdentifierExpression.of("sales.medium.type"),
                              STARTS_WITH,
                              ConstantExpression.of("distriBUTION")))
                      .identifierExpression(IdentifierExpression.of("sales.medium"))
                      .preserveNullAndEmptyArrays(false)
                      .build())
              .setFilter(
                  Filter.builder()
                      .expression(
                          RelationalExpression.of(
                              IdentifierExpression.of("item"),
                              STARTS_WITH,
                              ConstantExpression.of("S")))
                      .build())
              .build();

      final Iterator<Document> resultDocs = collection.aggregate(query);
      assertDocsAndSizeEqualWithoutOrder(datastoreName, resultDocs, "query/empty_response.json", 0);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/unwind_not_preserving_selection_response.json", 11);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/unwind_not_preserving_filter_response.json", 3);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, resultDocs, "query/unwind_preserving_selection_response.json", 17);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, iterator, "query/unwind_preserving_empty_array_response.json", 1);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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
        dataStoreName, iterator, "query/unwind_not_preserving_empty_array_response.json", 1);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
  public void testUnnestWithRegularFilterAndNullAndEmptyPreservedAtSecondLevel(String dataStoreName)
      throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("quantity"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addSelection(IdentifierExpression.of("sales.medium.type"))
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales"), true))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales.medium"))
                    .preserveNullAndEmptyArrays(true)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium.type"),
                            EQ,
                            ConstantExpression.of("retail")))
                    .build())
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium.type"),
                            EQ,
                            ConstantExpression.of("retail")))
                    .build())
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqual(
        dataStoreName, iterator, "query/unwind_preserve_with_regular_filter_second_level.json", 2);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testUnnestWithRegularFilterAndNullAndEmptyPreservedAtFirstLevel(String dataStoreName)
      throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("quantity"))
            .addSelection(IdentifierExpression.of("sales.city"))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(true)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.city"),
                            EQ,
                            ConstantExpression.of("mumbai")))
                    .build())
            .addFromClause(UnnestExpression.of(IdentifierExpression.of("sales.medium"), true))
            .setFilter(
                LogicalExpression.builder()
                    .operator(AND)
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("quantity"), GT, ConstantExpression.of(5)))
                    .operand(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.city"),
                            EQ,
                            ConstantExpression.of("mumbai")))
                    .build())
            .build();

    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqual(
        dataStoreName, iterator, "query/unwind_preserve_with_regular_filter_first_level.json", 3);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  void testContainsAndUnnestFilters(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("sales.medium"))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(false)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium"),
                            CONTAINS,
                            ConstantExpression.of(
                                new JSONDocument("{\"type\": \"retail\",\"volume\": 500}"))))
                    .build())
            .build();
    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqual(
        dataStoreName, iterator, "query/unwind_contains_filter_response.json", 3);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  void testNotContainsAndUnnestFilters(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("sales.medium"))
            .addFromClause(
                UnnestExpression.builder()
                    .identifierExpression(IdentifierExpression.of("sales"))
                    .preserveNullAndEmptyArrays(false)
                    .filterTypeExpression(
                        RelationalExpression.of(
                            IdentifierExpression.of("sales.medium"),
                            NOT_CONTAINS,
                            ConstantExpression.of(
                                new JSONDocument("{\"type\": \"retail\",\"volume\": 500}"))))
                    .build())
            .build();
    Iterator<Document> iterator = collection.aggregate(query);
    assertDocsAndSizeEqual(
        dataStoreName, iterator, "query/unwind_not_contains_filter_response.json", 2);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
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
  @ArgumentsSource(AllProvider.class)
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

  @Nested
  class KeyFilterTest {
    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
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
    @ArgumentsSource(AllProvider.class)
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
    @ArgumentsSource(AllProvider.class)
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
    @ArgumentsSource(AllProvider.class)
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
          datastoreName, resultDocs, "query/key_filter_multiple_response.json", 2);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
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
    @ArgumentsSource(AllProvider.class)
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
    @ArgumentsSource(AllProvider.class)
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
    @ArgumentsSource(AllProvider.class)
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
    @ArgumentsSource(AllProvider.class)
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
  }

  @Nested
  class AtomicUpdateTest {
    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
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
          "query/atomic_update_response.json",
          2);
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
    @ArgumentsSource(AllProvider.class)
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
          "query/atomic_update_response_get_new_document.json",
          2);
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
    @ArgumentsSource(AllProvider.class)
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
          "query/atomic_update_same_document_response.json",
          2);
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
    @ArgumentsSource(AllProvider.class)
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
          "query/updatable_collection_data_without_selection.json",
          9);
    }
  }

  @Nested
  class UpdateOperatorTest {
    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testUpdateWithAllOperators(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final SubDocumentUpdate set =
          SubDocumentUpdate.builder()
              .subDocument("props.new_property.deep.nested.value")
              .operator(SET)
              .subDocumentValue(SubDocumentValue.of("new_value"))
              .build();
      final SubDocumentUpdate unset =
          SubDocumentUpdate.builder().subDocument("sales").operator(UNSET).build();
      final SubDocumentUpdate add =
          SubDocumentUpdate.builder()
              .subDocument("props.added.set")
              .operator(ADD_TO_LIST_IF_ABSENT)
              .subDocumentValue(SubDocumentValue.of(new Integer[] {5, 1, 5}))
              .build();
      final SubDocumentUpdate another_add =
          SubDocumentUpdate.builder()
              .subDocument("props.planets")
              .operator(ADD_TO_LIST_IF_ABSENT)
              .subDocumentValue(SubDocumentValue.of(new String[] {"Neptune", "Pluto"}))
              .build();
      final SubDocumentUpdate append =
          SubDocumentUpdate.builder()
              .subDocument("props.appended.list")
              .operator(APPEND_TO_LIST)
              .subDocumentValue(SubDocumentValue.of(new Integer[] {1, 2}))
              .build();
      final SubDocumentUpdate remove =
          SubDocumentUpdate.builder()
              .subDocument("props.removed.list")
              .operator(REMOVE_ALL_FROM_LIST)
              .subDocumentValue(SubDocumentValue.of(new String[] {"Hello"}))
              .build();
      final SubDocumentUpdate increment =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates =
          List.of(set, unset, add, another_add, append, remove, increment);

      final CloseableIterator<Document> iterator =
          collection.bulkUpdate(
              query, updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, iterator, "query/update_operator/updated1.json", 9);

      final SubDocumentUpdate set_new =
          SubDocumentUpdate.builder()
              .subDocument("props.sales")
              .operator(SET)
              .subDocumentValue(SubDocumentValue.of("new_value"))
              .build();
      final SubDocumentUpdate unset_new =
          SubDocumentUpdate.builder()
              .subDocument("props.new_property.deep.nested")
              .operator(UNSET)
              .build();
      final SubDocumentUpdate add_new =
          SubDocumentUpdate.builder()
              .subDocument("props.added.set")
              .operator(ADD_TO_LIST_IF_ABSENT)
              .subDocumentValue(SubDocumentValue.of(new Integer[] {3, 1, 1000}))
              .build();
      final SubDocumentUpdate append_new =
          SubDocumentUpdate.builder()
              .subDocument("props.appended.list")
              .operator(APPEND_TO_LIST)
              .subDocumentValue(SubDocumentValue.of(new Integer[] {8, 2}))
              .build();
      final SubDocumentUpdate remove_new =
          SubDocumentUpdate.builder()
              .subDocument("props.planets")
              .operator(REMOVE_ALL_FROM_LIST)
              .subDocumentValue(SubDocumentValue.of(new String[] {"Pluto", "Mars"}))
              .build();
      final SubDocumentUpdate decrement =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(-1))
              .build();
      final List<SubDocumentUpdate> new_updates =
          List.of(set_new, unset_new, add_new, append_new, remove_new, decrement);

      final CloseableIterator<Document> iterator_new =
          collection.bulkUpdate(
              query, new_updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, iterator_new, "query/update_operator/updated2.json", 9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testUpdateWithOnlyAddUpdateOperator(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final SubDocumentUpdate increment =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();
      final SubDocumentUpdate incrementNested =
          SubDocumentUpdate.builder()
              .subDocument("itemCategory.item1.item2.item3.itemValue")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates = List.of(increment, incrementNested);

      final CloseableIterator<Document> iterator =
          collection.bulkUpdate(
              query, updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, iterator, "query/update_operator/add_updated1.json", 9);

      final SubDocumentUpdate decrement =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(-1))
              .build();
      final SubDocumentUpdate decrementNested =
          SubDocumentUpdate.builder()
              .subDocument("itemCategory.item1.item2.item3.itemValue")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(-1))
              .build();
      final List<SubDocumentUpdate> new_updates = List.of(decrement, decrementNested);

      final CloseableIterator<Document> iterator_new =
          collection.bulkUpdate(
              query, new_updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, iterator_new, "query/update_operator/add_updated2.json", 9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testUpdateWithAllOperatorsOnObject(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final SubDocumentUpdate set =
          SubDocumentUpdate.builder()
              .subDocument("props.new_property.deep.nested")
              .operator(SET)
              .subDocumentValue(SubDocumentValue.of(new JSONDocument(Map.of("value", "new_value"))))
              .build();
      final SubDocumentUpdate unset =
          SubDocumentUpdate.builder().subDocument("sales").operator(UNSET).build();
      final SubDocumentUpdate add =
          SubDocumentUpdate.builder()
              .subDocument("props.added.set")
              .operator(ADD_TO_LIST_IF_ABSENT)
              .subDocumentValue(
                  SubDocumentValue.of(
                      new Document[] {
                        new JSONDocument(Map.of("key", 1)),
                        new JSONDocument(Map.of("key", 2)),
                        new JSONDocument(Map.of("key", 1))
                      }))
              .build();
      final SubDocumentUpdate another_add =
          SubDocumentUpdate.builder()
              .subDocument("props.planets")
              .operator(ADD_TO_LIST_IF_ABSENT)
              .subDocumentValue(
                  SubDocumentValue.of(
                      new Document[] {
                        new JSONDocument(Map.of("name", "Neptune")),
                        new JSONDocument(Map.of("name", "Pluto"))
                      }))
              .build();
      final SubDocumentUpdate append =
          SubDocumentUpdate.builder()
              .subDocument("props.appended.list")
              .operator(APPEND_TO_LIST)
              .subDocumentValue(
                  SubDocumentValue.of(
                      new Document[] {
                        new JSONDocument(Map.of("key", 1)), new JSONDocument(Map.of("key", 2))
                      }))
              .build();
      final SubDocumentUpdate remove =
          SubDocumentUpdate.builder()
              .subDocument("props.removed.list")
              .operator(REMOVE_ALL_FROM_LIST)
              .subDocumentValue(
                  SubDocumentValue.of(new Document[] {new JSONDocument(Map.of("Hello", "world!"))}))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates = List.of(set, unset, add, another_add, append, remove);

      final CloseableIterator<Document> iterator =
          collection.bulkUpdate(
              query, updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, iterator, "query/update_operator/object_updated1.json", 9);

      final SubDocumentUpdate set_new =
          SubDocumentUpdate.builder()
              .subDocument("props.sales")
              .operator(SET)
              .subDocumentValue(
                  SubDocumentValue.of(new Document[] {new JSONDocument(Map.of("count", 789))}))
              .build();
      final SubDocumentUpdate unset_new =
          SubDocumentUpdate.builder()
              .subDocument("props.new_property.deep.nested")
              .operator(UNSET)
              .build();
      final SubDocumentUpdate add_new =
          SubDocumentUpdate.builder()
              .subDocument("props.added.set")
              .operator(ADD_TO_LIST_IF_ABSENT)
              .subDocumentValue(
                  SubDocumentValue.of(
                      new Document[] {
                        new JSONDocument(Map.of("key", 3)), new JSONDocument(Map.of("key", 1))
                      }))
              .build();
      final SubDocumentUpdate append_new =
          SubDocumentUpdate.builder()
              .subDocument("props.appended.list")
              .operator(APPEND_TO_LIST)
              .subDocumentValue(
                  SubDocumentValue.of(
                      new Document[] {
                        new JSONDocument(Map.of("key", 8)), new JSONDocument(Map.of("key", 2))
                      }))
              .build();
      final SubDocumentUpdate remove_new =
          SubDocumentUpdate.builder()
              .subDocument("props.planets")
              .operator(REMOVE_ALL_FROM_LIST)
              .subDocumentValue(
                  SubDocumentValue.of(
                      new Document[] {
                        new JSONDocument(Map.of("name", "Pluto")),
                        new JSONDocument(Map.of("name", "Mars"))
                      }))
              .build();

      final List<SubDocumentUpdate> new_updates =
          List.of(set_new, unset_new, add_new, append_new, remove_new);

      final CloseableIterator<Document> iterator_new =
          collection.bulkUpdate(
              query, new_updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, iterator_new, "query/update_operator/object_updated2.json", 9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testRemoveAllOccurrancesFromIntegerList(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final SubDocumentUpdate unset =
          SubDocumentUpdate.builder().subDocument("sales").operator(UNSET).build();
      final SubDocumentUpdate set =
          SubDocumentUpdate.builder()
              .subDocument("props.added.list")
              .operator(SET)
              .subDocumentValue(SubDocumentValue.of(new Integer[] {5, 1, 5}))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates = List.of(set, unset);

      final CloseableIterator<Document> iterator =
          collection.bulkUpdate(
              query, updates, UpdateOptions.builder().returnDocumentType(NONE).build());

      final SubDocumentUpdate remove =
          SubDocumentUpdate.builder()
              .subDocument("props.added.list")
              .operator(REMOVE_ALL_FROM_LIST)
              .subDocumentValue(SubDocumentValue.of(5))
              .build();
      final List<SubDocumentUpdate> new_updates = List.of(remove);

      final CloseableIterator<Document> iterator_new =
          collection.bulkUpdate(
              query, new_updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName,
          iterator_new,
          "query/update_operator/updated_removes_all_occurrances_from_integer_list.json",
          9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testAddToListIfAbsentDoesNotDeduplicateTheExistingList(final String datastoreName)
        throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final SubDocumentUpdate unset =
          SubDocumentUpdate.builder().subDocument("sales").operator(UNSET).build();
      final SubDocumentUpdate add =
          SubDocumentUpdate.builder()
              .subDocument("props.added.list")
              .subDocumentValue(SubDocumentValue.of(new Integer[] {5, 1, 5}))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates = List.of(add, unset);

      final CloseableIterator<Document> iterator =
          collection.bulkUpdate(
              query, updates, UpdateOptions.builder().returnDocumentType(NONE).build());

      final SubDocumentUpdate remove =
          SubDocumentUpdate.builder()
              .subDocument("props.added.list")
              .operator(ADD_TO_LIST_IF_ABSENT)
              .subDocumentValue(SubDocumentValue.of(new Integer[] {3, 1, 4}))
              .build();
      final List<SubDocumentUpdate> new_updates = List.of(remove);

      final CloseableIterator<Document> iterator_new =
          collection.bulkUpdate(
              query, new_updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName,
          iterator_new,
          "query/update_operator/updated_add_to_list_if_absent_does_not_deduplicate_existing_list.json",
          9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testSameHierarchyUpdateThrowsException(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final SubDocumentUpdate unset =
          SubDocumentUpdate.builder().subDocument("props.added").operator(UNSET).build();
      final SubDocumentUpdate add =
          SubDocumentUpdate.builder()
              .subDocument("props.added.list")
              .subDocumentValue(SubDocumentValue.of(new Integer[] {5, 1, 5}))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates = List.of(add, unset);

      assertThrows(
          IOException.class,
          () ->
              collection.bulkUpdate(
                  query, updates, UpdateOptions.builder().returnDocumentType(NONE).build()));
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testAddOperatorThrowExceptionForNonNumericValue(final String datastoreName)
        throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      // assert exception for string
      final SubDocumentUpdate addString =
          SubDocumentUpdate.builder()
              .subDocument("item")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of("Comb"))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates = List.of(addString);
      assertExceptionForNonNumericValues(collection, query, updates);

      // assert exception for list
      final SubDocumentUpdate addList =
          SubDocumentUpdate.builder()
              .subDocument("props.added.list")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(new Integer[] {5, 1, 5}))
              .build();
      final Query query_addList = Query.builder().build();
      final List<SubDocumentUpdate> updates_addList = List.of(addList);
      assertExceptionForNonNumericValues(collection, query_addList, updates_addList);

      // assert exception for Object
      final SubDocumentUpdate addObject =
          SubDocumentUpdate.builder()
              .subDocument("props.newObject")
              .operator(ADD)
              .subDocumentValue(
                  SubDocumentValue.of(
                      new Document[] {
                        new JSONDocument(Map.of("name", "Pluto")),
                        new JSONDocument(Map.of("name", "Mars"))
                      }))
              .build();
      final Query query_addObject = Query.builder().build();
      final List<SubDocumentUpdate> updates_addObject = List.of(addObject);
      assertExceptionForNonNumericValues(collection, query_addObject, updates_addObject);
    }

    private void assertExceptionForNonNumericValues(
        Collection collection, Query query, List<SubDocumentUpdate> updates) {
      assertThrows(
          IOException.class,
          () ->
              collection.bulkUpdate(
                  query, updates, UpdateOptions.builder().returnDocumentType(NONE).build()));
    }
  }

  @Nested
  class BulkUpdateTest {
    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testBulkUpdateWithFilterAndGetNoDocuments(final String datastoreName) throws IOException {
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
      final SubDocumentUpdate priceUpdate =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();
      final SubDocumentUpdate addProperty =
          SubDocumentUpdate.of(
              "props.new_property.deep.nested.value",
              SubDocumentValue.of(new JSONDocument("{\"json\": \"new_value\"}")));

      final CloseableIterator<Document> docIterator =
          collection.bulkUpdate(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate, addProperty, priceUpdate),
              UpdateOptions.builder().returnDocumentType(NONE).build());

      assertFalse(docIterator.hasNext());
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
          "query/bulk_update/updated_collection_data.json",
          9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testBulkUpdateWithFilterAndGetAfterDocumentsEmpty(final String datastoreName)
        throws IOException {
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
      final SubDocumentUpdate priceUpdate =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();
      final SubDocumentUpdate addProperty =
          SubDocumentUpdate.of(
              "props.new_property.deep.nested.value",
              SubDocumentValue.of(new JSONDocument("{\"json\": \"new_value\"}")));

      final CloseableIterator<Document> docIterator =
          collection.bulkUpdate(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate, addProperty, priceUpdate),
              UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());

      // Since the date is updated to conflict with the filter, there will not be any documents
      assertFalse(docIterator.hasNext());
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
          "query/bulk_update/updated_collection_data.json",
          9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testBulkUpdateWithFilterAndGetAfterDocumentsNonEmpty(final String datastoreName)
        throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"), EQ, ConstantExpression.of("Soap")))
              .addSort(SortingSpec.of(IdentifierExpression.of("price"), ASC))
              .addSort(SortingSpec.of(IdentifierExpression.of("props.size"), DESC))
              .addSelection(IdentifierExpression.of("quantity"))
              .addSelection(IdentifierExpression.of("price"))
              .addSelection(IdentifierExpression.of("date"))
              .addSelection(IdentifierExpression.of("props"))
              .build();
      final SubDocumentUpdate dateUpdate = SubDocumentUpdate.of("date", "2022-08-09T18:53:17Z");
      final SubDocumentUpdate quantityUpdate = SubDocumentUpdate.of("quantity", 1000);
      final SubDocumentUpdate propsUpdate = SubDocumentUpdate.of("props.brand", "Dettol");
      final SubDocumentUpdate priceUpdate =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();
      final SubDocumentUpdate addProperty =
          SubDocumentUpdate.of(
              "props.new_property.deep.nested.value",
              SubDocumentValue.of(new JSONDocument("{\"json\": \"new_value\"}")));

      final CloseableIterator<Document> docIterator =
          collection.bulkUpdate(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate, addProperty, priceUpdate),
              UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());

      assertDocsAndSizeEqual(
          datastoreName,
          docIterator,
          "query/bulk_update/updated_collection_response_after_update.json",
          4);
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
          "query/bulk_update/updated_collection_data_relaxed_filter.json",
          9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testBulkUpdateWithFilterAndGetBeforeDocuments(final String datastoreName)
        throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"), EQ, ConstantExpression.of("Soap")))
              .addSort(SortingSpec.of(IdentifierExpression.of("price"), ASC))
              .addSort(SortingSpec.of(IdentifierExpression.of("props.size"), DESC))
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
      final SubDocumentUpdate priceUpdate =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();

      final CloseableIterator<Document> docIterator =
          collection.bulkUpdate(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate, addProperty, priceUpdate),
              UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());

      assertDocsAndSizeEqual(
          datastoreName,
          docIterator,
          "query/bulk_update/updated_collection_response_before_update.json",
          4);
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
          "query/bulk_update/updated_collection_data_relaxed_filter.json",
          9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testBulkUpdateWithNonMatchingFilterAndGetBeforeDocuments(final String datastoreName)
        throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final Query query =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      EQ,
                      ConstantExpression.of("Non-existing-item")))
              .addSort(SortingSpec.of(IdentifierExpression.of("price"), ASC))
              .addSort(SortingSpec.of(IdentifierExpression.of("props.size"), DESC))
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
      final SubDocumentUpdate priceUpdate =
          SubDocumentUpdate.builder()
              .subDocument("price")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();

      final CloseableIterator<Document> docIterator =
          collection.bulkUpdate(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate, addProperty, priceUpdate),
              UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());

      assertFalse(docIterator.hasNext());
      assertDocsAndSizeEqual(
          datastoreName,
          collection.find(Query.builder().addSort(IdentifierExpression.of("_id"), ASC).build()),
          "query/bulk_update/updatable_collection_data_no_update.json",
          9);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testExistsOperatorWithFindUsingStringRhs(String dataStoreName) throws Exception {
    Collection collection = getCollection(dataStoreName);

    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();
    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("props"), EXISTS, ConstantExpression.of("true")))
            .build();

    Query query = Query.builder().setSelection(selection).setFilter(filter).build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, "query/exists_filter_response.json", 4);

    testCountApi(dataStoreName, query, "query/exists_filter_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testExistsOperatorWithFindUsingBooleanRhs(String dataStoreName) throws Exception {
    Collection collection = getCollection(dataStoreName);

    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();
    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("props"), EXISTS, ConstantExpression.of(true)))
            .build();

    Query query = Query.builder().setSelection(selection).setFilter(filter).build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, "query/exists_filter_response.json", 4);

    testCountApi(dataStoreName, query, "query/exists_filter_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testNotExistsOperatorWithFindUsingStringRhs(String dataStoreName) throws Exception {
    Collection collection = getCollection(dataStoreName);

    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();
    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("props"), NOT_EXISTS, ConstantExpression.of("true")))
            .build();

    Query query = Query.builder().setSelection(selection).setFilter(filter).build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, "query/not_exists_filter_response.json", 4);

    testCountApi(dataStoreName, query, "query/not_exists_filter_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testNotExistsOperatorWithFindUsingBooleanRhs(String dataStoreName) throws Exception {
    Collection collection = getCollection(dataStoreName);

    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(IdentifierExpression.of("item")),
            SelectionSpec.of(IdentifierExpression.of("date")));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();
    Filter filter =
        Filter.builder()
            .expression(
                RelationalExpression.of(
                    IdentifierExpression.of("props"), NOT_EXISTS, ConstantExpression.of(true)))
            .build();

    Query query = Query.builder().setSelection(selection).setFilter(filter).build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, "query/not_exists_filter_response.json", 4);

    testCountApi(dataStoreName, query, "query/not_exists_filter_response.json");
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
