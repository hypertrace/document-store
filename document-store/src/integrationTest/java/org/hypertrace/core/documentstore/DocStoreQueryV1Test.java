package org.hypertrace.core.documentstore;

import static java.util.Collections.emptyMap;
import static java.util.Map.entry;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.and;
import static org.hypertrace.core.documentstore.expression.impl.LogicalExpression.or;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.AVG;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_ARRAY;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.LAST;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MAX;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.MIN;
import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.DIVIDE;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.FLOOR;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.SUBTRACT;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.NOT;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.OR;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.CONTAINS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EXISTS;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.GTE;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LIKE;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.hypertrace.core.documentstore.commons.DocStoreConstants;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayType;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonFieldType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.operators.AggregationOperator;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.options.UpdateOptions.MissingDocumentStrategy;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;
import org.hypertrace.core.documentstore.query.Aggregation;
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
  private static final String FLAT_COLLECTION_NAME = "myTestFlat";
  private static final String PG_FLAT_COLLECTION_INSERT_STATMENTS_FILE_LOC =
      "query/pg_flat_collection_insert.json";

  private static Map<String, Datastore> datastoreMap;

  private static GenericContainer<?> mongo;
  private static GenericContainer<?> postgres;

  @BeforeAll
  public static void init() throws IOException {
    datastoreMap = Maps.newHashMap();
    mongo =
        new GenericContainer<>(DockerImageName.parse("mongo:8.0.1"))
            .withExposedPorts(27017)
            .waitingFor(Wait.forListeningPort());
    mongo.start();

    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.putIfAbsent("host", "localhost");
    mongoConfig.putIfAbsent("port", mongo.getMappedPort(27017).toString());
    mongoConfig.putIfAbsent("isSortOptimizedQueryEnabled", "true");
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

    createFlatCollectionSchema(datastoreMap.get(POSTGRES_STORE), FLAT_COLLECTION_NAME);
    executeInsertStatements((PostgresDatastore) datastoreMap.get(POSTGRES_STORE));
  }

  private static void createFlatCollectionSchema(
      Datastore postgresDatastore, String collectionName) {
    String createTableSQL =
        String.format(
            "CREATE TABLE \"%s\" ("
                + "\"_id\" INTEGER PRIMARY KEY,"
                + "\"item\" TEXT,"
                + "\"price\" INTEGER,"
                + "\"quantity\" INTEGER,"
                + "\"date\" TIMESTAMPTZ,"
                + "\"in_stock\" BOOLEAN,"
                + "\"tags\" TEXT[],"
                + "\"categoryTags\" TEXT[],"
                + "\"props\" JSONB,"
                + "\"sales\" JSONB,"
                + "\"numbers\" INTEGER[],"
                + "\"scores\" DOUBLE PRECISION[],"
                + "\"flags\" BOOLEAN[]"
                + ");",
            collectionName);

    // Access PostgresDatastore directly to get client connection
    PostgresDatastore pgDatastore = (PostgresDatastore) postgresDatastore;

    try {

      // Execute the CREATE TABLE SQL directly using the public getPostgresClient method
      try (java.sql.Connection connection = pgDatastore.getPostgresClient();
          java.sql.PreparedStatement statement = connection.prepareStatement(createTableSQL)) {
        statement.execute();
        System.out.println("Created flat collection table: " + collectionName);
      }
    } catch (Exception e) {
      System.err.println("Failed to create flat collection schema: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static void executeInsertStatements(PostgresDatastore pgDatastore) {
    try {
      // Read JSON file from resources
      String jsonContent =
          readFileFromResource(PG_FLAT_COLLECTION_INSERT_STATMENTS_FILE_LOC).orElseThrow();
      ObjectMapper mapper = new ObjectMapper();
      JsonNode rootNode = mapper.readTree(jsonContent);

      // Extract statements array
      JsonNode statementsNode = rootNode.get("statements");
      if (statementsNode == null || !statementsNode.isArray()) {
        throw new RuntimeException("Invalid JSON format: 'statements' array not found");
      }

      try (java.sql.Connection connection = pgDatastore.getPostgresClient()) {
        for (com.fasterxml.jackson.databind.JsonNode statementNode : statementsNode) {
          String statement = statementNode.asText().trim();
          if (!statement.isEmpty()) {
            try (java.sql.PreparedStatement preparedStatement =
                connection.prepareStatement(statement)) {
              preparedStatement.executeUpdate();
            }
          }
        }
      }
    } catch (Exception e) {
      System.err.println("Failed to execute INSERT statements: " + e.getMessage());
    }
  }

  private static void createCollectionData(final String resourcePath, final String collectionName)
      throws IOException {
    final Map<Key, Document> documents = Utils.buildDocumentsFromResource(resourcePath);
    datastoreMap.forEach(
        (k, v) -> {
          v.deleteCollection(collectionName);
          // for Postgres, we also create the flat collection
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

  /**
   * Provides arguments for testing array operations with different expression types. Returns:
   * (datastoreName, expressionType) - "WITH_TYPE": ArrayIdentifierExpression WITH ArrayType
   * (optimized, type-aware casting) - "WITHOUT_TYPE": ArrayIdentifierExpression WITHOUT ArrayType
   * (fallback, text[] casting)
   */
  private static class PostgresArrayTypeProvider implements ArgumentsProvider {

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext context) {
      return Stream.of(
          Arguments.of(POSTGRES_STORE, "WITH_TYPE"), // ArrayIdentifierExpression WITH ArrayType
          Arguments.of(
              POSTGRES_STORE, "WITHOUT_TYPE") // ArrayIdentifierExpression WITHOUT ArrayType
          );
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
  void testAggregateWithNestedArraysAndUnnestFilters(final String dataStoreName)
      throws IOException {
    final Datastore datastore = datastoreMap.get(dataStoreName);
    final Collection collection = datastore.getCollection(COLLECTION_NAME);
    final FilterTypeExpression filter =
        LogicalExpression.builder()
            .operator(AND)
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("item"), NEQ, ConstantExpression.of((String) null)))
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("sales.medium.type"),
                    EQ,
                    ConstantExpression.of("distributionChannel")))
            .operand(
                RelationalExpression.of(
                    IdentifierExpression.of("price"), LT, ConstantExpression.of(20)))
            .build();
    final Query query =
        Query.builder()
            .setSelections(
                List.of(
                    SelectionSpec.of(IdentifierExpression.of("item")),
                    SelectionSpec.of(IdentifierExpression.of("sales.city")),
                    SelectionSpec.of(IdentifierExpression.of("price")),
                    SelectionSpec.of(IdentifierExpression.of("sales.medium.type")),
                    SelectionSpec.of(IdentifierExpression.of("sales.medium.volume"))))
            .setFilter(filter)
            .addFromClauses(
                List.of(
                    UnnestExpression.builder()
                        .preserveNullAndEmptyArrays(false)
                        .identifierExpression(IdentifierExpression.of("sales"))
                        .filterTypeExpression(filter)
                        .build(),
                    UnnestExpression.builder()
                        .preserveNullAndEmptyArrays(false)
                        .identifierExpression(IdentifierExpression.of("sales.medium"))
                        .filterTypeExpression(filter)
                        .build()))
            .build();

    final Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqual(
        dataStoreName, resultDocs, "query/test_aggr_nested_arrays_and_unnest_filters.json", 4);
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
  public void testQueryV1FilterWithNestedField(String dataStoreName) throws IOException {
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
  public void testQueryV1AggregationWithInFilterWithPrimitiveLhs(final String dataStoreName)
      throws IOException {
    final Collection collection = getCollection(dataStoreName);
    final Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("quantity"))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("item"),
                    IN,
                    ConstantExpression.ofStrings(List.of("Comb", "Shampoo"))))
            .build();

    final Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, "query/test_primitive_lhs_in_filter_aggr_response.json", 4);
  }

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testQueryV1AggregationWithInFilterWithArrayLhs(final String dataStoreName)
      throws IOException {
    final Collection collection = getCollection(dataStoreName);
    final Query query =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("price"))
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("props.colors"),
                    IN,
                    ConstantExpression.ofStrings(List.of("Orange"))))
            .build();

    final Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName,
        resultDocs,
        "query/test_json_column_array_lhs_in_filter_aggr_response.json",
        1);
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

  @ParameterizedTest
  @ArgumentsSource(MongoProvider.class)
  public void testQueryV1LastAggregationOperator(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addAggregation(IdentifierExpression.of("item"))
            .addSelection(
                AggregateExpression.of(LAST, IdentifierExpression.of("price")), "last_price")
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, "query/test_last_aggregation_operator.json", 4);
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

  @Nested
  class ContainsOperatorTest {

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    public void testContains(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName);

      final org.hypertrace.core.documentstore.query.Query query =
          org.hypertrace.core.documentstore.query.Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("price"))
              .addSelection(IdentifierExpression.of("props.colors"))
              .setFilter(
                  Filter.builder()
                      .expression(
                          RelationalExpression.of(
                              IdentifierExpression.of("props.colors"),
                              CONTAINS,
                              ConstantExpression.of("Green")))
                      .build())
              .build();

      final Iterator<Document> resultDocs = collection.aggregate(query);
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, resultDocs, "query/contains_filter_response.json", 1);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    public void testNotContains(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName);

      final org.hypertrace.core.documentstore.query.Query query =
          org.hypertrace.core.documentstore.query.Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("price"))
              .addSelection(IdentifierExpression.of("props.colors"))
              .setFilter(
                  Filter.builder()
                      .expression(
                          RelationalExpression.of(
                              IdentifierExpression.of("props.colors"),
                              NOT_CONTAINS,
                              ConstantExpression.of("Green")))
                      .build())
              .build();

      final Iterator<Document> resultDocs = collection.aggregate(query);
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, resultDocs, "query/not_contains_filter_response.json", 7);
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
  class AtomicCreateOrReplaceTest {

    private static final String CREATE_OR_REPLACE_COLLECTION = "createOrReplaceCollection";

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    public void testAtomicCreateOrReplace(final String datastoreName)
        throws IOException, ExecutionException, InterruptedException {
      final Collection collection = getCollection(datastoreName, CREATE_OR_REPLACE_COLLECTION);
      final Key key = Key.from(UUID.randomUUID().toString());

      final Document document1 =
          new JSONDocument(readFileFromResource("create/document_one.json").orElseThrow());
      final Document document2 =
          new JSONDocument(readFileFromResource("create/document_two.json").orElseThrow());

      final Random random = new Random();
      final Callable<Boolean> callable1 =
          () -> {
            MILLISECONDS.sleep(random.nextInt(1000));
            return collection.createOrReplace(key, document1);
          };

      final Callable<Boolean> callable2 =
          () -> {
            MILLISECONDS.sleep(random.nextInt(1000));
            return collection.createOrReplace(key, document2);
          };

      final ExecutorService executor = Executors.newFixedThreadPool(2);

      final boolean document1Created = executor.submit(callable1).get();
      final boolean document2Created = executor.submit(callable2).get();

      final CloseableIterator<Document> iterator =
          collection.aggregate(
              Query.builder()
                  .setFilter(Filter.builder().expression(KeyExpression.of(key)).build())
                  .build());
      final List<Document> documents =
          StreamSupport.stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .collect(toUnmodifiableList());

      assertEquals(1, documents.size());
      @SuppressWarnings("unchecked")
      final Map<String, Object> mapping =
          new ObjectMapper().readValue(documents.get(0).toJson(), Map.class);
      assertTrue(
          (long) mapping.get(DocStoreConstants.LAST_UPDATED_TIME)
              > (long) mapping.get(DocStoreConstants.CREATED_TIME));

      if (document1Created) {
        assertFalse(document2Created);
        // If document 1 was created, document 2 should have replaced it
        assertDocsAndSizeEqual(
            datastoreName, documents.iterator(), "create/document_two_response.json", 1);
      } else {
        assertTrue(document2Created);
        // If document 1 was not created, document 2 should have been created and then, document 1
        // should have replaced it
        assertDocsAndSizeEqual(
            datastoreName, documents.iterator(), "create/document_one_response.json", 1);
      }
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    public void testAtomicCreateOrReplaceAndReturn(final String datastoreName)
        throws IOException, ExecutionException, InterruptedException {
      final Collection collection = getCollection(datastoreName, CREATE_OR_REPLACE_COLLECTION);
      final Key key = Key.from(UUID.randomUUID().toString());

      final Document document1 =
          new JSONDocument(readFileFromResource("create/document_one.json").orElseThrow());
      final Document document2 =
          new JSONDocument(readFileFromResource("create/document_two.json").orElseThrow());

      final Random random = new Random();
      final Callable<Document> callable1 =
          () -> {
            MILLISECONDS.sleep(random.nextInt(1000));
            return collection.createOrReplaceAndReturn(key, document1);
          };

      final Callable<Document> callable2 =
          () -> {
            MILLISECONDS.sleep(random.nextInt(1000));
            return collection.createOrReplaceAndReturn(key, document2);
          };

      final ExecutorService executor = Executors.newFixedThreadPool(2);

      final Document document1Created = executor.submit(callable1).get();
      final Document document2Created = executor.submit(callable2).get();

      assertDocsAndSizeEqual(
          datastoreName,
          List.of(document1Created).iterator(),
          "create/document_one_response.json",
          1);
      assertDocsAndSizeEqual(
          datastoreName,
          List.of(document2Created).iterator(),
          "create/document_two_response.json",
          1);
    }
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

  @ParameterizedTest
  @ArgumentsSource(AllProvider.class)
  public void testAggregateWithZeroLimitAndOffset(final String datastoreName) throws IOException {
    final Collection collection = getCollection(datastoreName);

    final Iterator<Document> resultDocs =
        collection.aggregate(
            Query.builder().setPagination(Pagination.builder().limit(0).offset(0).build()).build());
    assertDocsAndSizeEqual(datastoreName, resultDocs, "query/empty_response.json", 0);
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

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    public void testAtomicUpdateWithReturnDocumentTypeNone(final String datastoreName)
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
      final SubDocumentUpdate propsUpdate =
          SubDocumentUpdate.of(
              "props", SubDocumentValue.of(new JSONDocument("{\"brand\": \"Dettol\"}")));

      // Perform update with ReturnDocumentType.NONE
      final Optional<Document> updateResult =
          collection.update(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate),
              UpdateOptions.builder().returnDocumentType(NONE).build());

      // Verify that no document is returned (as expected with NONE)
      assertFalse(
          updateResult.isPresent(), "Should return empty Optional when ReturnDocumentType.NONE");

      // Verify that the document was actually updated in the database by querying it
      final Query verificationQuery =
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
                              EQ,
                              ConstantExpression.of("2022-08-09T18:53:17Z")))
                      .build())
              .build();

      final CloseableIterator<Document> verificationResults = collection.find(verificationQuery);
      assertTrue(verificationResults.hasNext(), "Updated document should exist in database");

      final Document updatedDocument = verificationResults.next();
      final JsonNode json = new ObjectMapper().readTree(updatedDocument.toJson());
      // Verify that the right document was updated in the database
      assertEquals(null, json.get("sales"), "'sales' field should not be present");

      // Verify the fields were updated
      assertEquals(1000, json.get("quantity").asInt(), "Quantity should be updated to 1000");
      assertEquals("2022-08-09T18:53:17Z", json.get("date").asText(), "Date should be updated");
      final JsonNode props = json.get("props");
      assertEquals(
          "Dettol", props.get("brand").asText(), "Props brand should be updated to Dettol");

      verificationResults.close();

      // Additional test: Verify that the same update with AFTER_UPDATE returns the document
      // Reset the data first
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final Optional<Document> afterUpdateResult =
          collection.update(
              query,
              List.of(dateUpdate, quantityUpdate, propsUpdate),
              UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());

      // This should return the updated document
      assertTrue(
          afterUpdateResult.isPresent(),
          "Should return document when ReturnDocumentType.AFTER_UPDATE");

      final Document returnedDocument = afterUpdateResult.get();
      final JsonNode returnedJson = new ObjectMapper().readTree(returnedDocument.toJson());
      assertEquals(1000, returnedJson.get("quantity").asInt());
      assertEquals("2022-08-09T18:53:17Z", returnedJson.get("date").asText());
    }
  }

  @Nested
  class UpdateOperatorTest {

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testUpdateSetEmptyObject(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final SubDocumentUpdate set =
          SubDocumentUpdate.builder()
              .subDocument("props.new_property.with.empty.object")
              .operator(SET)
              .subDocumentValue(
                  SubDocumentValue.of(
                      new JSONDocument(
                          Map.ofEntries(
                              entry("hello", "world"), entry("emptyObject", emptyMap())))))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates = List.of(set);

      final CloseableIterator<Document> iterator =
          collection.bulkUpdate(
              query, updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());
      assertDocsAndSizeEqualWithoutOrder(
          datastoreName, iterator, "query/update_operator/updated3.json", 9);
    }

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
    @ArgumentsSource(MongoProvider.class)
    void testUpdateWithUpsertOptions(final String dataStoreName) throws IOException {
      final Collection collection = getCollection(dataStoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);
      final SubDocumentUpdate set =
          SubDocumentUpdate.builder()
              .subDocument("props.brand")
              .operator(SET)
              .subDocumentValue(SubDocumentValue.of(new JSONDocument(Map.of("value", "nike"))))
              .build();
      Filter filter =
          Filter.builder()
              .expression(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      IN,
                      ConstantExpression.ofStrings(List.of("shoes"))))
              .build();

      final Query query = Query.builder().setFilter(filter).build();
      final List<SubDocumentUpdate> updates = List.of(set);
      final CloseableIterator<Document> iterator =
          collection.bulkUpdate(
              query,
              updates,
              UpdateOptions.builder()
                  .returnDocumentType(AFTER_UPDATE)
                  .missingDocumentStrategy(MissingDocumentStrategy.CREATE_USING_UPDATES)
                  .build());
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, iterator, "query/update_operator/updated4.json", 1);
      Filter filter1 =
          Filter.builder()
              .expression(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      IN,
                      ConstantExpression.ofStrings(List.of("shirt"))))
              .build();
      final Query query1 = Query.builder().setFilter(filter1).build();
      final SubDocumentUpdate add =
          SubDocumentUpdate.builder()
              .subDocument("quantity")
              .operator(ADD)
              .subDocumentValue(SubDocumentValue.of(1))
              .build();
      final List<SubDocumentUpdate> update1 = List.of(add);
      final CloseableIterator<Document> iterator1 =
          collection.bulkUpdate(
              query1,
              update1,
              UpdateOptions.builder()
                  .returnDocumentType(AFTER_UPDATE)
                  .missingDocumentStrategy(MissingDocumentStrategy.CREATE_USING_UPDATES)
                  .build());
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, iterator1, "query/update_operator/updated5.json", 1);
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
    void testRemoveFromSingletonList(final String datastoreName) throws IOException {
      final Collection collection = getCollection(datastoreName, UPDATABLE_COLLECTION_NAME);
      createCollectionData("query/updatable_collection_data.json", UPDATABLE_COLLECTION_NAME);

      final SubDocumentUpdate set =
          SubDocumentUpdate.builder()
              .subDocument("props.added.habitable_planets")
              .operator(SET)
              .subDocumentValue(SubDocumentValue.of(new String[] {"Earth"}))
              .build();

      final Query query = Query.builder().build();
      final List<SubDocumentUpdate> updates = List.of(set);

      final CloseableIterator<Document> iterator =
          collection.bulkUpdate(
              query, updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());

      assertDocsAndSizeEqualWithoutOrder(
          datastoreName,
          iterator,
          "query/update_operator/updated_set_string_array_with_singleton_element.json",
          9);

      final SubDocumentUpdate remove =
          SubDocumentUpdate.builder()
              .subDocument("props.added.habitable_planets")
              .operator(REMOVE_ALL_FROM_LIST)
              .subDocumentValue(SubDocumentValue.of(new String[] {"Earth"}))
              .build();

      final List<SubDocumentUpdate> newUpdates = List.of(remove);

      final CloseableIterator<Document> newIterator =
          collection.bulkUpdate(
              query, newUpdates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build());

      assertDocsAndSizeEqualWithoutOrder(
          datastoreName,
          newIterator,
          "query/update_operator/updated_remove_from_string_array_with_singleton_element.json",
          9);
    }

    @ParameterizedTest
    @ArgumentsSource(AllProvider.class)
    void testRemoveAllOccurrencesFromIntegerList(final String datastoreName) throws IOException {
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
  class FlatPostgresCollectionGeneralQueries {

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFindAll(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test basic query to retrieve all documents
      Query query = Query.builder().build();
      CloseableIterator<Document> iterator = flatCollection.find(query);

      // Count documents
      long count = 0;
      while (iterator.hasNext()) {
        Document doc = iterator.next();
        count++;
        // Verify document has content (basic validation)
        assertNotNull(doc);
        assertNotNull(doc.toJson());
        assertTrue(!doc.toJson().isEmpty());
        assertEquals(DocumentType.FLAT, doc.getDocumentType());
      }
      iterator.close();
      // Should have 10 documents from the INSERT statements
      assertEquals(10, count);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testUnnestPreserveEmptyArraysFalse(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query unnestQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("tags"))
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of("*")), "count")
              .addAggregation(IdentifierExpression.of("tags"))
              .addFromClause(UnnestExpression.of(IdentifierExpression.of("tags"), false))
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(unnestQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_unnest_tags_response.json", 17);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testUnnestPreserveEmptyArraysTrue(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Include all documents in result irrespective of tags field (LEFT JOIN)
      // Counts rows after unnest: 25 (from 8 docs with tags) + 2 (from docs with NULL/empty)
      Query unnestPreserveTrueQuery =
          Query.builder()
              .addSelection(AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count")
              .addFromClause(UnnestExpression.of(IdentifierExpression.of("tags"), true))
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(unnestPreserveTrueQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName,
          resultIterator,
          "query/flat_unnest_preserving_empty_array_response.json",
          1);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFlatVsNestedCollectionConsistency(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);

      // Get both collection types
      Collection nestedCollection =
          datastore.getCollection(COLLECTION_NAME); // Default nested collection
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test 1: Count all documents
      // Flat collection has 10 docs (8 matching nested + 2 for NULL/empty array testing)
      // Nested collection has 8 docs
      Query countAllQuery = Query.builder().build();
      long nestedCount = nestedCollection.count(countAllQuery);
      long flatCount = flatCollection.count(countAllQuery);
      assertEquals(8, nestedCount, "Nested collection should have 8 documents");
      assertEquals(10, flatCount, "Flat collection should have 10 documents");

      // Test 2: Filter by top-level field - item
      Query itemFilterQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"), EQ, ConstantExpression.of("Soap")))
              .build();

      long nestedSoapCount = nestedCollection.count(itemFilterQuery);
      long flatSoapCount = flatCollection.count(itemFilterQuery);
      assertEquals(
          nestedSoapCount, flatSoapCount, "Soap count should be equal in both collections");

      // Test 3: Filter by numeric field - price
      // Nested has 2 docs with price > 10 (Mirror=20, Soap=20)
      // Flat has 3 docs with price > 10 (Mirror=20, Soap=20, Bottle=15)
      Query priceFilterQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("price"), GT, ConstantExpression.of(10)))
              .build();

      long nestedPriceCount = nestedCollection.count(priceFilterQuery);
      long flatPriceCount = flatCollection.count(priceFilterQuery);
      assertEquals(2, nestedPriceCount, "Nested should have 2 docs with price > 10");
      assertEquals(3, flatPriceCount, "Flat should have 3 docs with price > 10");

      // Test 4: Compare actual document content for same filter
      CloseableIterator<Document> nestedIterator = nestedCollection.find(itemFilterQuery);
      CloseableIterator<Document> flatIterator = flatCollection.find(itemFilterQuery);

      // Collect documents from both collections
      java.util.List<String> nestedDocs = new java.util.ArrayList<>();
      java.util.List<String> flatDocs = new java.util.ArrayList<>();

      while (nestedIterator.hasNext()) {
        nestedDocs.add(nestedIterator.next().toJson());
      }
      nestedIterator.close();

      while (flatIterator.hasNext()) {
        flatDocs.add(flatIterator.next().toJson());
      }
      flatIterator.close();

      // Both should return the same number of documents
      assertEquals(
          nestedDocs.size(),
          flatDocs.size(),
          "Both collections should return same number of documents");

      // Test 5: Verify document structure consistency
      if (!nestedDocs.isEmpty() && !flatDocs.isEmpty()) {
        // Parse and compare first document from each collection
        ObjectMapper mapper = new ObjectMapper();
        JsonNode nestedDoc = mapper.readTree(nestedDocs.get(0));
        JsonNode flatDoc = mapper.readTree(flatDocs.get(0));

        // Verify both have the same top-level fields
        assertTrue(nestedDoc.has("item") && flatDoc.has("item"), "Both should have 'item' field");
        assertTrue(
            nestedDoc.has("price") && flatDoc.has("price"), "Both should have 'price' field");
        assertTrue(
            nestedDoc.has("quantity") && flatDoc.has("quantity"),
            "Both should have 'quantity' field");
        assertTrue(nestedDoc.has("date") && flatDoc.has("date"), "Both should have 'date' field");

        // Verify the values are the same for basic fields
        assertEquals(
            nestedDoc.get("item").asText(),
            flatDoc.get("item").asText(),
            "Item values should match");
        assertEquals(
            nestedDoc.get("price").asInt(),
            flatDoc.get("price").asInt(),
            "Price values should match");
        assertEquals(
            nestedDoc.get("quantity").asInt(),
            flatDoc.get("quantity").asInt(),
            "Quantity values should match");
      }

      // Test 6: Test with different filter - quantity
      Query quantityFilterQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("quantity"), EQ, ConstantExpression.of(10)))
              .build();

      long nestedQuantityCount = nestedCollection.count(quantityFilterQuery);
      long flatQuantityCount = flatCollection.count(quantityFilterQuery);
      assertEquals(
          nestedQuantityCount,
          flatQuantityCount,
          "Quantity = 10 count should be equal in both collections");

      // Test 7: Verify DocumentType is different
      CloseableIterator<Document> nestedDocIterator =
          nestedCollection.find(Query.builder().build());
      CloseableIterator<Document> flatDocIterator = flatCollection.find(Query.builder().build());

      if (nestedDocIterator.hasNext() && flatDocIterator.hasNext()) {
        Document nestedDocument = nestedDocIterator.next();
        Document flatDocument = flatDocIterator.next();

        assertEquals(
            DocumentType.NESTED,
            nestedDocument.getDocumentType(),
            "Nested collection should return NESTED documents");
        assertEquals(
            DocumentType.FLAT,
            flatDocument.getDocumentType(),
            "Flat collection should return FLAT documents");
      }

      nestedDocIterator.close();
      flatDocIterator.close();
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFlatPostgresCollectionUnnestWithComplexQuery(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Tests UNNEST with WHERE filter (price >= 5), unnest filter (NOT LIKE 'home-%'),
      // GROUP BY tags, HAVING (count > 1), ORDER BY count DESC
      Query complexQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("tags"))
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of("*")), "tag_count")
              .addSelection(
                  AggregateExpression.of(AVG, IdentifierExpression.of("price")), "avg_price")
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("price"), GTE, ConstantExpression.of(5)))
              .addFromClause(
                  UnnestExpression.builder()
                      .identifierExpression(IdentifierExpression.of("tags"))
                      .preserveNullAndEmptyArrays(false)
                      .filterTypeExpression(
                          LogicalExpression.builder()
                              .operator(NOT)
                              .operand(
                                  RelationalExpression.of(
                                      IdentifierExpression.of("tags"),
                                      LIKE,
                                      ConstantExpression.of("home-%")))
                              .build())
                      .build())
              .addAggregation(IdentifierExpression.of("tags"))
              .setAggregationFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("tag_count"), GT, ConstantExpression.of(1)))
              .addSort(SortingSpec.of(IdentifierExpression.of("tag_count"), DESC))
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(complexQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_unnest_complex_query_response.json", 7);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFlatPostgresCollectionUnnestWithOnlyUnnestFilter(String dataStoreName)
        throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Query with unnest filter but NO main WHERE filter
      Query unnestFilterOnlyQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("tags"))
              .addFromClause(
                  UnnestExpression.builder()
                      .identifierExpression(IdentifierExpression.of("tags"))
                      .preserveNullAndEmptyArrays(false)
                      .filterTypeExpression(
                          RelationalExpression.of(
                              IdentifierExpression.of("tags"),
                              EQ,
                              ConstantExpression.of("premium")))
                      .build())
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(unnestFilterOnlyQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_unnest_only_unnest_filter_response.json", 2);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFlatPostgresCollectionUnnestWithOnlyMainFilter(String dataStoreName)
        throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Query with main WHERE filter but NO unnest filter
      Query mainFilterOnlyQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("tags"))
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("price"), GT, ConstantExpression.of(10)))
              .addFromClause(
                  UnnestExpression.builder()
                      .identifierExpression(IdentifierExpression.of("tags"))
                      .preserveNullAndEmptyArrays(false)
                      .build())
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(mainFilterOnlyQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_unnest_only_main_filter_response.json", 6);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFlatPostgresCollectionArrayRelationalFilter(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Filter: ANY tag in tags equals "hygiene" AND _id <= 8
      // Exclude docs 9-10 (NULL/empty arrays) to avoid ARRAY[] type error
      Query arrayRelationalQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("price"))
              .setFilter(
                  LogicalExpression.builder()
                      .operator(LogicalOperator.AND)
                      .operand(
                          ArrayRelationalFilterExpression.builder()
                              .operator(ArrayOperator.ANY)
                              .filter(
                                  RelationalExpression.of(
                                      IdentifierExpression.of("tags"),
                                      EQ,
                                      ConstantExpression.of("hygiene")))
                              .build())
                      .operand(
                          RelationalExpression.of(
                              IdentifierExpression.of("_id"), LTE, ConstantExpression.of(8)))
                      .build())
              .build();

      Iterator<Document> resultIterator = flatCollection.find(arrayRelationalQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_array_relational_filter_response.json", 3);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFlatPostgresCollectionUnnestMixedCaseField(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test UNNEST on field with mixed case: categoryTags
      // This will create alias "categoryTags_unnested" which must be quoted to preserve case
      Query unnestQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("categoryTags"))
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of("*")), "count")
              .addAggregation(IdentifierExpression.of("categoryTags"))
              .addFromClause(UnnestExpression.of(IdentifierExpression.of("categoryTags"), false))
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(unnestQuery);
      // Expected categories: Hygiene(3), PersonalCare(2), HairCare(2), HomeDecor(1), Grooming(2)
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_unnest_mixed_case_response.json", 5);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFlatVsNestedCollectionNestedFieldSelections(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);

      Collection nestedCollection = datastore.getCollection(COLLECTION_NAME);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test 1: Select nested field - props.brand
      // Nested collection uses dot notation
      Query nestedBrandQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("props.brand"), "brand")
              .addSort(IdentifierExpression.of("item"), ASC)
              .build();

      // Flat collection uses JsonIdentifierExpression
      // Filter to exclude docs 9-10 to match nested collection dataset
      Query flatBrandQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(JsonIdentifierExpression.of("props", "brand"), "brand")
              .addSort(IdentifierExpression.of("item"), ASC)
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("_id"), LTE, ConstantExpression.of(8)))
              .build();

      // Assert both match the expected response
      Iterator<Document> nestedBrandIterator = nestedCollection.find(nestedBrandQuery);
      assertDocsAndSizeEqual(dataStoreName, nestedBrandIterator, "query/brand_response.json", 8);

      Iterator<Document> flatBrandIterator = flatCollection.find(flatBrandQuery);
      assertDocsAndSizeEqual(dataStoreName, flatBrandIterator, "query/brand_response.json", 8);

      // Test 2: Select nested JSON array - props.colors
      Query nestedColorsQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("props.colors"), "colors")
              .addSort(IdentifierExpression.of("item"), ASC)
              .build();

      // Filter to exclude docs 9-10 to match nested collection dataset
      Query flatColorsQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(JsonIdentifierExpression.of("props", "colors"), "colors")
              .addSort(IdentifierExpression.of("item"), ASC)
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("_id"), LTE, ConstantExpression.of(8)))
              .build();

      // Assert both match the expected response
      Iterator<Document> nestedColorsIterator = nestedCollection.find(nestedColorsQuery);
      assertDocsAndSizeEqual(dataStoreName, nestedColorsIterator, "query/colors_response.json", 8);

      Iterator<Document> flatColorsIterator = flatCollection.find(flatColorsQuery);
      assertDocsAndSizeEqual(dataStoreName, flatColorsIterator, "query/colors_response.json", 8);

      // Test 3: Select nested field WITHOUT alias - should preserve full nested structure
      Query nestedBrandNoAliasQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("props.brand"))
              .addSort(IdentifierExpression.of("item"), ASC)
              .build();

      // Filter to exclude docs 9-10 to match nested collection dataset
      Query flatBrandNoAliasQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(JsonIdentifierExpression.of("props", "brand"))
              .addSort(IdentifierExpression.of("item"), ASC)
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("_id"), LTE, ConstantExpression.of(8)))
              .build();

      // Assert both match the expected response with nested structure
      Iterator<Document> nestedBrandNoAliasIterator =
          nestedCollection.find(nestedBrandNoAliasQuery);
      assertDocsAndSizeEqual(
          dataStoreName, nestedBrandNoAliasIterator, "query/no_alias_response.json", 8);

      Iterator<Document> flatBrandNoAliasIterator = flatCollection.find(flatBrandNoAliasQuery);
      assertDocsAndSizeEqual(
          dataStoreName, flatBrandNoAliasIterator, "query/no_alias_response.json", 8);
    }

    /**
     * Tests that GROUP BY with UNNEST on JSONB array fields produces consistent results across
     * nested and flat collections. This validates that both collection types properly unnest arrays
     * and group by individual elements (not entire arrays).
     */
    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testFlatVsNestedCollectionGroupByArrayField(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);

      Collection nestedCollection = datastore.getCollection(COLLECTION_NAME);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Nested collection: GROUP BY with UNNEST on props.colors array
      // Uses dot notation for nested collections
      Query nestedGroupByQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("props.colors"), "color")
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "count")
              .addFromClause(UnnestExpression.of(IdentifierExpression.of("props.colors"), false))
              .addAggregation(IdentifierExpression.of("props.colors"))
              .addSort(IdentifierExpression.of("props.colors"), ASC)
              .build();

      // Flat collection: GROUP BY with UNNEST on props.colors array
      // Uses JsonIdentifierExpression for JSONB columns
      Query flatGroupByQuery =
          Query.builder()
              .addSelection(JsonIdentifierExpression.of("props", "colors"), "color")
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "count")
              .addFromClause(
                  UnnestExpression.of(JsonIdentifierExpression.of("props", "colors"), false))
              .addAggregation(JsonIdentifierExpression.of("props", "colors"))
              .addSort(JsonIdentifierExpression.of("props", "colors"), ASC)
              .build();

      // Execute queries
      Iterator<Document> nestedResultIterator = nestedCollection.aggregate(nestedGroupByQuery);
      Iterator<Document> flatResultIterator = flatCollection.aggregate(flatGroupByQuery);

      // Both should produce the same results: grouping by individual color elements
      // Expected: Black (1), Blue (2), Green (1), Orange (1)
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, nestedResultIterator, "query/group_by_colors_comparison_response.json", 4);

      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, flatResultIterator, "query/group_by_colors_comparison_response.json", 4);
    }
  }

  @Nested
  class FlatCollectionScalarColumns {

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testGroupBy(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test GROUP BY on scalar field (item) with COUNT aggregation
      Query groupByQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of("*")), "count")
              .addAggregation(IdentifierExpression.of("item"))
              .build();

      Iterator<Document> results = flatCollection.aggregate(groupByQuery);

      int groupCount = 0;
      Map<String, Integer> itemCounts = new HashMap<>();
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        groupCount++;

        String item = json.get("item").asText();
        int count = json.get("count").asInt();
        itemCounts.put(item, count);
      }

      assertTrue(groupCount > 0);

      // Verify Soap appears 3 times (IDs 1, 5, 8)
      assertEquals(3, itemCounts.getOrDefault("Soap", 0));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testAllRelationalOps(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test NEQ (Not Equal) on string field
      Query neqQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"), NEQ, ConstantExpression.of("Soap")))
              .build();
      long neqCount = flatCollection.count(neqQuery);
      assertEquals(7, neqCount); // 10 total - 3 Soap = 7

      // Test LT (Less Than) on integer field
      Query ltQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("price"), LT, ConstantExpression.of(10)))
              .build();
      long ltCount = flatCollection.count(ltQuery);
      assertTrue(ltCount > 0); // Should have prices < 10

      // Test LTE (Less Than or Equal) on integer field
      Query lteQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("price"), LTE, ConstantExpression.of(10)))
              .build();
      long lteCount = flatCollection.count(lteQuery);
      assertTrue(lteCount >= ltCount); // LTE should include LT results

      // Test GTE (Greater Than or Equal) on integer field
      Query gteQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("quantity"), GTE, ConstantExpression.of(5)))
              .build();
      long gteCount = flatCollection.count(gteQuery);
      assertTrue(gteCount > 0);

      // Test IN operator on string field
      Query inQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      IN,
                      ConstantExpression.ofStrings(List.of("Soap", "Mirror", "Comb"))))
              .build();
      long inCount = flatCollection.count(inQuery);
      assertEquals(6, inCount); // 3 Soap + 1 Mirror + 2 Comb = 6

      // Test NOT_IN operator on string field
      Query notInQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"),
                      NOT_IN,
                      ConstantExpression.ofStrings(List.of("Soap", "Mirror"))))
              .build();
      long notInCount = flatCollection.count(notInQuery);
      assertEquals(6, notInCount); // 10 total - 3 Soap - 1 Mirror = 6

      // Test LIKE operator on string field (pattern matching)
      Query likeQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"), LIKE, ConstantExpression.of(".*amp.*")))
              .build();
      long likeCount = flatCollection.count(likeQuery);
      assertEquals(2, likeCount); // Should match "Shampoo" (IDs 3, 4)

      // Test STARTS_WITH operator on string field
      Query startsWithQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"), STARTS_WITH, ConstantExpression.of("S")))
              .build();
      long startsWithCount = flatCollection.count(startsWithQuery);
      assertEquals(5, startsWithCount); // "Soap" (3) + "Shampoo" (2) = 5

      // Test combined operators with AND logic
      Query combinedQuery =
          Query.builder()
              .setFilter(
                  LogicalExpression.builder()
                      .operator(LogicalOperator.AND)
                      .operand(
                          RelationalExpression.of(
                              IdentifierExpression.of("price"), GTE, ConstantExpression.of(5)))
                      .operand(
                          RelationalExpression.of(
                              IdentifierExpression.of("quantity"), LTE, ConstantExpression.of(10)))
                      .operand(
                          RelationalExpression.of(
                              IdentifierExpression.of("in_stock"), EQ, ConstantExpression.of(true)))
                      .build())
              .build();
      long combinedCount = flatCollection.count(combinedQuery);
      assertTrue(combinedCount > 0);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testSorting(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test 1: Sort by string field ASC
      Query sortItemAscQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("price"))
              .addSort(IdentifierExpression.of("item"), ASC)
              .build();

      Iterator<Document> sortItemAscResults = flatCollection.find(sortItemAscQuery);
      String previousItem = null;
      int count = 0;
      while (sortItemAscResults.hasNext()) {
        Document doc = sortItemAscResults.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        String currentItem = json.get("item").asText();
        if (previousItem != null) {
          assertTrue(
              currentItem.compareTo(previousItem) >= 0,
              "Items should be sorted in ascending order");
        }
        previousItem = currentItem;
        count++;
      }
      assertEquals(10, count);

      // Test 2: Sort by integer field DESC
      Query sortPriceDescQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("price"))
              .addSort(IdentifierExpression.of("price"), DESC)
              .build();

      Iterator<Document> sortPriceDescResults = flatCollection.find(sortPriceDescQuery);
      Integer previousPrice = null;
      count = 0;
      while (sortPriceDescResults.hasNext()) {
        Document doc = sortPriceDescResults.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        int currentPrice = json.get("price").asInt();
        if (previousPrice != null) {
          assertTrue(currentPrice <= previousPrice, "Prices should be sorted in descending order");
        }
        previousPrice = currentPrice;
        count++;
      }
      assertEquals(10, count);

      // Test 3: Multi-level sort (item ASC, then price DESC)
      Query multiSortQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("price"))
              .addSort(IdentifierExpression.of("item"), ASC)
              .addSort(IdentifierExpression.of("price"), DESC)
              .build();

      Iterator<Document> multiSortResults = flatCollection.find(multiSortQuery);
      String prevItem = null;
      Integer prevPrice = null;
      while (multiSortResults.hasNext()) {
        Document doc = multiSortResults.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        String currentItem = json.get("item").asText();
        int currentPrice = json.get("price").asInt();

        if (prevItem != null) {
          if (currentItem.equals(prevItem)) {
            // Same item, price should be descending
            assertTrue(currentPrice <= prevPrice, "Within same item, price should descend");
          } else {
            // Different item, should be ascending
            assertTrue(currentItem.compareTo(prevItem) >= 0, "Items should be sorted ascending");
          }
        }
        prevItem = currentItem;
        prevPrice = currentPrice;
      }
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNumericAggregations(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test SUM, AVG, MIN, MAX, COUNT on integer fields
      Query aggQuery =
          Query.builder()
              .addSelection(AggregateExpression.of(SUM, IdentifierExpression.of("price")), "sum")
              .addSelection(AggregateExpression.of(AVG, IdentifierExpression.of("price")), "avg")
              .addSelection(AggregateExpression.of(MIN, IdentifierExpression.of("price")), "min")
              .addSelection(AggregateExpression.of(MAX, IdentifierExpression.of("price")), "max")
              .addSelection(
                  AggregateExpression.of(COUNT, IdentifierExpression.of("price")), "count")
              .build();

      Iterator<Document> aggResults = flatCollection.aggregate(aggQuery);
      assertTrue(aggResults.hasNext());

      Document aggDoc = aggResults.next();
      JsonNode json = new ObjectMapper().readTree(aggDoc.toJson());

      // Validate aggregation results
      assertTrue(json.get("sum").asDouble() > 0, "SUM should be positive");
      assertTrue(json.get("avg").asDouble() > 0, "AVG should be positive");
      assertTrue(json.get("min").asDouble() > 0, "MIN should be positive");
      assertTrue(json.get("max").asDouble() > 0, "MAX should be positive");
      assertEquals(10, json.get("count").asInt(), "COUNT should be 10");

      // Verify MIN <= AVG <= MAX
      double min = json.get("min").asDouble();
      double avg = json.get("avg").asDouble();
      double max = json.get("max").asDouble();
      assertTrue(min <= avg, "MIN should be <= AVG");
      assertTrue(avg <= max, "AVG should be <= MAX");

      // Test GROUP BY with aggregations
      Query groupAggQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(
                  AggregateExpression.of(SUM, IdentifierExpression.of("quantity")), "total_qty")
              .addSelection(
                  AggregateExpression.of(AVG, IdentifierExpression.of("price")), "avg_price")
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of("*")), "count")
              .addAggregation(IdentifierExpression.of("item"))
              .addSort(IdentifierExpression.of("item"), ASC)
              .build();

      Iterator<Document> groupAggResults = flatCollection.aggregate(groupAggQuery);
      int groupCount = 0;
      while (groupAggResults.hasNext()) {
        Document doc = groupAggResults.next();
        JsonNode groupJson = new ObjectMapper().readTree(doc.toJson());
        groupCount++;

        // Validate each group has all expected fields
        assertNotNull(groupJson.get("item"));
        assertTrue(groupJson.get("total_qty").asInt() > 0);
        assertTrue(groupJson.get("avg_price").asDouble() > 0);
        assertTrue(groupJson.get("count").asInt() > 0);
      }
      assertTrue(groupCount > 0, "Should have at least one group");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNullHandling(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Note: All scalar fields (item, price, quantity, in_stock) have non-NULL values
      // in existing data. This test validates correct handling when no NULLs are present.

      // Test 1: Verify all items are non-NULL
      Query notNullQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"), NEQ, ConstantExpression.of("null")))
              .build();

      long notNullCount = flatCollection.count(notNullQuery);
      assertEquals(10, notNullCount);

      // Test 2: Verify COUNT(field) equals COUNT(*) when no NULLs present
      Query aggQuery =
          Query.builder()
              .addSelection(
                  AggregateExpression.of(COUNT, IdentifierExpression.of("item")), "count_item")
              .addSelection(
                  AggregateExpression.of(COUNT, IdentifierExpression.of("price")), "count_price")
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of("*")), "count_all")
              .build();

      Iterator<Document> aggResults = flatCollection.aggregate(aggQuery);
      assertTrue(aggResults.hasNext());

      Document aggDoc = aggResults.next();
      JsonNode json = new ObjectMapper().readTree(aggDoc.toJson());

      // When no NULLs, COUNT(field) should equal COUNT(*)
      int countItem = json.get("count_item").asInt();
      int countPrice = json.get("count_price").asInt();
      int countAll = json.get("count_all").asInt();

      assertEquals(10, countItem, "COUNT(item) should be 10");
      assertEquals(10, countPrice, "COUNT(price) should be 10");
      assertEquals(10, countAll, "COUNT(*) should be 10");
      assertEquals(countItem, countAll, "COUNT(item) should equal COUNT(*) when no NULLs");
      assertEquals(countPrice, countAll, "COUNT(price) should equal COUNT(*) when no NULLs");

      // Test 3: Test NULL equality filter returns empty result
      Query nullEqualQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      IdentifierExpression.of("item"), EQ, ConstantExpression.of("null")))
              .build();

      long nullCount = flatCollection.count(nullEqualQuery);
      assertEquals(0, nullCount);
    }
  }

  @Nested
  class FlatCollectionTopLevelArrayColumns {

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotEmpty(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("tags"))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags"), EXISTS, ConstantExpression.of("null")))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;
        JsonNode tags = json.get("tags");
        assertTrue(tags.isArray() && !tags.isEmpty());
      }
      // (Ids 1 to 8 have non-empty tags)
      assertEquals(8, count);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testEmpty(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(IdentifierExpression.of("tags"))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags"),
                      NOT_EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        count++;
      }

      // (Ids 9 and 10 have NULL or EMPTY arrays)
      assertEquals(2, count);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testUnnest(String dataStoreName) {

      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      for (boolean preserveNullAndEmpty : List.of(true, false)) {
        Query unnestQuery =
            Query.builder()
                .addSelection(IdentifierExpression.of("item"))
                .addSelection(
                    JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "source-loc"))
                .addFromClause(
                    UnnestExpression.of(
                        JsonIdentifierExpression.of(
                            "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                        preserveNullAndEmpty))
                .build();

        Iterator<Document> resultIterator = flatCollection.find(unnestQuery);
        int count = 0;
        while (resultIterator.hasNext()) {
          Document doc = resultIterator.next();
          assertNotNull(doc);
          count++;
        }
        // With preserveNullAndEmpty = false, unnest will only unwind arrays with size >= 1. A total
        // of 6 such rows would be created from the 3 arrays in the table
        // With true, it'll include NULL and EMPTY arrays too. This will result in 13 rows
        assertTrue(preserveNullAndEmpty ? count == 13 : count == 6);
      }
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testInStringArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query inQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("tags"))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                      IN,
                      ConstantExpression.ofStrings(List.of("hygiene", "grooming"))))
              .build();

      Iterator<Document> results = flatCollection.find(inQuery);

      int count = 0;
      Set<String> items = new HashSet<>();
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        String item = json.get("item").asText();
        items.add(item);

        // Verify that returned arrays contain at least one of the IN values
        JsonNode tags = json.get("tags");
        if (tags != null && tags.isArray()) {
          boolean containsMatch = false;
          for (JsonNode tag : tags) {
            String tagValue = tag.asText();
            if ("hygiene".equals(tagValue) || "grooming".equals(tagValue)) {
              containsMatch = true;
              break;
            }
          }
          assertTrue(containsMatch, "Array should contain at least one IN value for item: " + item);
        }
      }

      // Should return rows where tags array overlaps with ["hygiene", "grooming"]
      // hygiene: IDs 1, 5, 8 (Soap), 6, 7 (Comb)
      assertTrue(count >= 5, "Should return at least 5 items");
      assertTrue(items.contains("Soap"));
      assertTrue(items.contains("Comb"));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotInStringArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test NOT_IN on native array WITHOUT unnest
      // This should use NOT (array overlap) to check arrays don't contain any of the values
      Query notInQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("tags"))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                      NOT_IN,
                      ConstantExpression.ofStrings(List.of("premium", "hygiene"))))
              .build();

      Iterator<Document> results = flatCollection.find(notInQuery);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        // Verify that returned arrays do NOT contain any of the NOT_IN values
        JsonNode tags = json.get("tags");
        if (tags != null && tags.isArray() && !tags.isEmpty()) {
          for (JsonNode tag : tags) {
            String tagValue = tag.asText();
            assertNotEquals(
                "premium",
                tagValue,
                "tags array should NOT contain 'premium' for item: " + json.get("item").asText());
            assertNotEquals(
                "hygiene",
                tagValue,
                "tags array should NOT contain 'hygiene' for item: " + json.get("item").asText());
          }
        }
      }

      // Should return rows where tags array does NOT overlap with ["premium", "hygiene"]
      // Rows 1, 3, 5, 8 have hygiene or premium, so should be excluded
      // Should return: Mirror, Shampoo (id 4), Comb
      assertTrue(count >= 3, "Should return at least 3 items without premium/hygiene tags");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testInIntArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test IN on integer array (numbers column)
      Query inQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("numbers"))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("numbers", ArrayType.INTEGER),
                      IN,
                      ConstantExpression.ofNumbers(List.of(1, 10, 20))))
              .build();

      Iterator<Document> results = flatCollection.find(inQuery);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        // Verify that returned arrays contain at least one of the IN values
        JsonNode numbers = json.get("numbers");
        if (numbers != null && numbers.isArray()) {
          boolean containsMatch = false;
          for (JsonNode num : numbers) {
            int value = num.asInt();
            if (value == 1 || value == 10 || value == 20) {
              containsMatch = true;
              break;
            }
          }
          assertTrue(
              containsMatch,
              "Array should contain at least one IN value for item: " + json.get("item").asText());
        }
      }

      // Should return rows where numbers array overlaps with [1, 10, 20]
      // IDs: 1 {1,2,3}, 2 {10,20}, 3 {5,10,15}, 6 {20,30}, 7 {10}, 8 {1,10,20}
      assertTrue(count >= 6, "Should return at least 6 items");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testInDoubleArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test IN on double precision array (scores column)
      Query inQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("scores"))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("scores", ArrayType.DOUBLE_PRECISION),
                      IN,
                      ConstantExpression.ofNumbers(List.of(3.14, 5.0))))
              .build();

      Iterator<Document> results = flatCollection.find(inQuery);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        // Verify that returned arrays contain at least one of the IN values
        JsonNode scores = json.get("scores");
        if (scores != null && scores.isArray()) {
          boolean containsMatch = false;
          for (JsonNode score : scores) {
            double value = score.asDouble();
            if (Math.abs(value - 3.14) < 0.01 || Math.abs(value - 5.0) < 0.01) {
              containsMatch = true;
              break;
            }
          }
          assertTrue(
              containsMatch,
              "Array should contain at least one IN value for item: " + json.get("item").asText());
        }
      }

      // Should return rows where scores array overlaps with [3.14, 5.0]
      // IDs: 3 {3.14,2.71}, 4 {5.0,10.0}, 8 {2.5,5.0}
      assertTrue(count >= 3, "Should return at least 3 items");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testInWithUnnest(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      for (boolean preserveNullAndEmptyArrays : List.of(true, false)) {
        Query unnestQuery =
            Query.builder()
                .addSelection(IdentifierExpression.of("item"))
                .addSelection(ArrayIdentifierExpression.of("tags"))
                .addFromClause(
                    UnnestExpression.of(
                        ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                        preserveNullAndEmptyArrays))
                // Should return unnested tag elements that match 'hygiene' OR 'grooming'
                .setFilter(
                    RelationalExpression.of(
                        ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                        IN,
                        ConstantExpression.ofStrings(List.of("hygiene", "grooming"))))
                .build();

        // this query will first unnest "tags" array and keep rows that have null and empty
        // arrays. It'll then filter those rows for which the
        // unnested tag is either hygiene or grooming. We have a total of 5 rows that'll match
        // this filter
        Iterator<Document> results = flatCollection.find(unnestQuery);

        int count = 0;
        while (results.hasNext()) {
          Document doc = results.next();
          assertNotNull(doc);
          count++;
        }
        assertEquals(5, count, "Should return at least one unnested tag matching the filter");
      }
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotInWithUnnest(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      for (boolean preserveNullAndEmptyArrays : List.of(true, false)) {
        Query unnestQuery =
            Query.builder()
                .addSelection(IdentifierExpression.of("item"))
                .addSelection(ArrayIdentifierExpression.of("tags"))
                .addFromClause(
                    UnnestExpression.of(
                        ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                        preserveNullAndEmptyArrays))
                .setFilter(
                    RelationalExpression.of(
                        ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                        NOT_IN,
                        ConstantExpression.ofStrings(List.of("hygiene", "grooming"))))
                .build();
        // this query will first unnest "tags" array and keep rows that have null and empty
        // arrays. unnest() on empty and null arrays returns NULL which is then
        // included in the result set (as the predicate contains tags_unnested == NULL OR ...)

        Iterator<Document> results = flatCollection.find(unnestQuery);

        int count = 0;
        while (results.hasNext()) {
          Document doc = results.next();
          assertNotNull(doc);
          count++;
        }
        assertEquals(
            preserveNullAndEmptyArrays ? 22 : 20,
            count,
            "Should return unnested tags not matching the filter");
      }
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testEmptyWithUnnest(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query unnestQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("tags"))
              .addFromClause(
                  UnnestExpression.of(ArrayIdentifierExpression.of("tags", ArrayType.TEXT), true))
              // Only include tags[] that are either NULL or empty (we have one row with NULL tag
              // and one with empty tag. Unnest will result in two rows with NULL for
              // "tags_unnested"). Note that this behavior will change with
              // preserveNulLAndEmptyArrays = false. This is because unnest won't preserve those
              // rows for which the unnested column is NULL then.
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                      NOT_EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> results = flatCollection.find(unnestQuery);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        assertNotNull(doc);
        count++;
      }

      assertEquals(2, count, "Should return at least 2 rows with NULL unnested tags");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotEmptyWithUnnest(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query unnestQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("tags"))
              .addFromClause(
                  UnnestExpression.of(ArrayIdentifierExpression.of("tags", ArrayType.TEXT), true))
              // Only include tags[] that have at least 1 element, all rows with NULL or empty tags
              // should be excluded.
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                      EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> results = flatCollection.find(unnestQuery);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        assertNotNull(doc);
        count++;
      }

      assertEquals(25, count, "Should return unnested tag elements from non-empty arrays");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testContainsStrArrayWithUnnest(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addFromClause(
                  UnnestExpression.of(ArrayIdentifierExpression.of("tags", ArrayType.TEXT), true))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                      CONTAINS,
                      ConstantExpression.ofStrings(List.of("hygiene", "premium"))))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        results.next();
        count++;
      }
      assertEquals(5, count);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testContainsStrArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("tags", ArrayType.TEXT))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                      CONTAINS,
                      ConstantExpression.ofStrings(List.of("hygiene", "personal-care"))))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      Set<String> items = new HashSet<>();
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        String item = json.get("item").asText();
        items.add(item);

        // Verify that returned arrays contain both "hygiene" AND "personal-care"
        JsonNode tags = json.get("tags");
        assertTrue(tags.isArray(), "tags should be an array");
        boolean containsHygiene = false;
        boolean containsPersonalCare = false;
        for (JsonNode tag : tags) {
          if ("hygiene".equals(tag.asText())) {
            containsHygiene = true;
          }
          if ("personal-care".equals(tag.asText())) {
            containsPersonalCare = true;
          }
        }
        assertTrue(containsHygiene);
        assertTrue(containsPersonalCare);
      }

      assertEquals(1, count);
      assertTrue(items.contains("Soap"));
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotContainsStrArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("tags"))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("tags", ArrayType.TEXT),
                      NOT_CONTAINS,
                      ConstantExpression.ofStrings(List.of("hair-care", "personal-care"))))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      Set<String> items = new HashSet<>();
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;
        items.add(json.get("item").asText());

        // Verify that returned arrays do NOT contain BOTH "hair-care" AND "personal-care"
        JsonNode tags = json.get("tags");
        if (tags != null && tags.isArray() && !tags.isEmpty()) {
          boolean hasHairCare = false;
          boolean hasPersonalCare = false;
          for (JsonNode tag : tags) {
            if ("hair-care".equals(tag.asText())) {
              hasHairCare = true;
            }
            if ("personal-care".equals(tag.asText())) {
              hasPersonalCare = true;
            }
          }
          assertFalse(hasHairCare && hasPersonalCare);
        }
      }

      assertEquals(9, count);
      assertNotEquals(2, items.stream().filter("Shampoo"::equals).count());
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testContainsOnIntArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("numbers", ArrayType.INTEGER))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("numbers", ArrayType.INTEGER),
                      CONTAINS,
                      ConstantExpression.ofNumbers(List.of(1, 2))))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        // Verify numbers field is a proper JSON array, not a PostgreSQL string like "{1,2,3}"
        JsonNode numbers = json.get("numbers");
        assertNotNull(numbers);
        assertTrue(numbers.isArray(), "numbers should be JSON array, got: " + numbers);

        // Verify array contains both 1 and 2
        boolean contains1 = false;
        boolean contains2 = false;
        for (JsonNode num : numbers) {
          if (num.asInt() == 1) {
            contains1 = true;
          }
          if (num.asInt() == 2) {
            contains2 = true;
          }
        }
        assertTrue(contains1);
        assertTrue(contains2);
      }

      assertEquals(2, count);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotContainsOnIntArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("numbers", ArrayType.INTEGER))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("numbers", ArrayType.INTEGER),
                      NOT_CONTAINS,
                      ConstantExpression.ofNumbers(List.of(10, 20))))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        // Verify numbers field is a proper JSON array
        JsonNode numbers = json.get("numbers");
        if (numbers != null && !numbers.isNull()) {
          assertTrue(numbers.isArray());

          // Verify array does NOT contain BOTH 10 AND 20
          boolean has10 = false;
          boolean has20 = false;
          for (JsonNode num : numbers) {
            if (num.asInt() == 10) {
              has10 = true;
            }
            if (num.asInt() == 20) {
              has20 = true;
            }
          }
          assertFalse(has10 && has20);
        }
      }

      // 8 rows: excludes rows 2 and 8 (have both 10 & 20)
      // Includes rows 9, 10 (NULL) because NOT_CONTAINS uses "IS NULL OR NOT (...)" logic
      assertEquals(8, count);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testContainsOnDoubleArray(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(ArrayIdentifierExpression.of("scores", ArrayType.DOUBLE_PRECISION))
              .setFilter(
                  RelationalExpression.of(
                      ArrayIdentifierExpression.of("scores", ArrayType.DOUBLE_PRECISION),
                      CONTAINS,
                      ConstantExpression.ofNumbers(List.of(3.14, 2.71))))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        JsonNode scores = json.get("scores");
        assertNotNull(scores);
        assertTrue(scores.isArray(), "scores should be JSON array, got: " + scores);

        boolean contains314 = false;
        boolean contains271 = false;
        for (JsonNode score : scores) {
          double val = score.asDouble();
          if (val == 3.14) {
            contains314 = true;
          }
          if (val == 2.71) {
            contains271 = true;
          }
        }
        assertTrue(contains314);
        assertTrue(contains271);
      }

      assertEquals(1, count);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testAnyOnIntegerArray(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query integerArrayQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .setFilter(
                  ArrayRelationalFilterExpression.builder()
                      .operator(ArrayOperator.ANY)
                      .filter(
                          RelationalExpression.of(
                              IdentifierExpression.of("numbers"), EQ, ConstantExpression.of(10)))
                      .build())
              .build();

      Iterator<Document> resultIterator = flatCollection.find(integerArrayQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_integer_array_filter_response.json", 4);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testAnyOnDoubleArray(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query doubleArrayQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .setFilter(
                  ArrayRelationalFilterExpression.builder()
                      .operator(ArrayOperator.ANY)
                      .filter(
                          RelationalExpression.of(
                              IdentifierExpression.of("scores"), EQ, ConstantExpression.of(3.14)))
                      .build())
              .build();

      Iterator<Document> resultIterator = flatCollection.find(doubleArrayQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_double_array_filter_response.json", 1);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testAnyOnBooleanArray(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query booleanArrayQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .setFilter(
                  ArrayRelationalFilterExpression.builder()
                      .operator(ArrayOperator.ANY)
                      .filter(
                          RelationalExpression.of(
                              IdentifierExpression.of("flags"), EQ, ConstantExpression.of(true)))
                      .build())
              .build();

      Iterator<Document> resultIterator = flatCollection.find(booleanArrayQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_boolean_array_filter_response.json", 5);
    }
  }

  @Nested
  class FlatCollectionJsonbColumns {

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testSelections(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query brandSelectionQuery =
          Query.builder()
              .addSelection(JsonIdentifierExpression.of("props", JsonFieldType.STRING, "brand"))
              .build();

      Iterator<Document> brandIterator = flatCollection.find(brandSelectionQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, brandIterator, "query/flat_jsonb_brand_selection_response.json", 10);

      // Test 2: Select deeply nested STRING field from JSONB column (props.seller.address.city)
      Query citySelectionQuery =
          Query.builder()
              .addSelection(
                  JsonIdentifierExpression.of(
                      "props", JsonFieldType.STRING, "seller", "address", "city"))
              .build();

      Iterator<Document> cityIterator = flatCollection.find(citySelectionQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, cityIterator, "query/flat_jsonb_city_selection_response.json", 10);

      // Test 3: Select STRING_ARRAY field from JSONB column (props.colors)
      Query colorsSelectionQuery =
          Query.builder()
              .addSelection(
                  JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors"))
              .build();

      Iterator<Document> colorsIterator = flatCollection.find(colorsSelectionQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, colorsIterator, "query/flat_jsonb_colors_selection_response.json", 10);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testRelOpArrayContains(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test 1: CONTAINS - props.colors CONTAINS "Green"
      // Expected: 1 document (id=1, Dettol Soap has ["Green", "White"])
      Query containsQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors"),
                      CONTAINS,
                      ConstantExpression.of("Green")))
              .build();

      long containsCount = flatCollection.count(containsQuery);
      // Generated query: SELECT COUNT(*) FROM (SELECT * FROM "myTestFlat" WHERE "props"->'colors'
      // @> ('["Green"]')::jsonb) p(countWithParser)
      assertEquals(1, containsCount);

      // Test 2: NOT_CONTAINS - props.colors NOT_CONTAINS "Green" AND _id <= 8
      // Expected: 7 documents (all except id=1 which has Green, limited to first 8)
      Query notContainsQuery =
          Query.builder()
              .setFilter(
                  LogicalExpression.builder()
                      .operator(LogicalOperator.AND)
                      .operand(
                          RelationalExpression.of(
                              JsonIdentifierExpression.of(
                                  "props", JsonFieldType.STRING_ARRAY, "colors"),
                              NOT_CONTAINS,
                              ConstantExpression.of("Green")))
                      .operand(
                          RelationalExpression.of(
                              IdentifierExpression.of("_id"), LTE, ConstantExpression.of(8)))
                      .build())
              .build();

      long notContainsCount = flatCollection.count(notContainsQuery);
      // Generated query: SELECT COUNT(*) FROM (SELECT * FROM "myTestFlat" WHERE ("props"->'colors'
      // IS NULL OR NOT "props"->'colors' @> ('["Green"]')::jsonb) AND ("_id" <= ('8'::int4)))
      // p(countWithParser)
      assertEquals(7, notContainsCount);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testRelOpArrayIN(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test 1: IN - props.brand IN ["Dettol", "Lifebuoy"]
      // Expected: 2 documents (id=1 Dettol, id=5 Lifebuoy)
      Query inQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING, "brand"),
                      IN,
                      ConstantExpression.ofStrings(List.of("Dettol", "Lifebuoy"))))
              .build();

      long inCount = flatCollection.count(inQuery);
      assertEquals(2, inCount);

      // Test 2: NOT_IN - props.brand NOT_IN ["Dettol"] AND _id <= 8
      // Expected: 7 documents (all except id=1 which is Dettol, limited to first 8)
      Query notInQuery =
          Query.builder()
              .setFilter(
                  LogicalExpression.builder()
                      .operator(LogicalOperator.AND)
                      .operand(
                          RelationalExpression.of(
                              JsonIdentifierExpression.of("props", JsonFieldType.STRING, "brand"),
                              NOT_IN,
                              ConstantExpression.ofStrings(List.of("Dettol"))))
                      .operand(
                          RelationalExpression.of(
                              IdentifierExpression.of("_id"), LTE, ConstantExpression.of(8)))
                      .build())
              .build();

      long notInCount = flatCollection.count(notInQuery);
      assertEquals(7, notInCount);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testRelOpScalarEq(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query eqQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING, "brand"),
                      EQ,
                      ConstantExpression.of("Dettol")))
              .build();

      long eqCount = flatCollection.count(eqQuery);
      // Generate query: SELECT COUNT(*) FROM (SELECT * FROM "myTestFlat" WHERE "props"->>'brand' =
      // ('Dettol')) p(countWithParser)
      assertEquals(1, eqCount);

      Query neqQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING, "brand"),
                      NEQ,
                      ConstantExpression.of("Dettol")))
              .build();

      long neqCount = flatCollection.count(neqQuery);
      // Generate query: SELECT COUNT(*) FROM (SELECT * FROM "myTestFlat" WHERE "props"->>'brand' !=
      // ('Dettol')) p(countWithParser)
      assertEquals(2, neqCount);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testRelOpScalarNumericComparison(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query gtQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.NUMBER, "seller", "address", "pincode"),
                      GT,
                      ConstantExpression.of(500000)))
              .build();

      long gtCount = flatCollection.count(gtQuery);
      // SELECT COUNT(*) FROM (SELECT * FROM "myTestFlat" WHERE CAST
      // ("props"->'seller'->'address'->>'pincode' AS NUMERIC) > ('500000'::int4))
      // p(countWithParser)
      assertEquals(2, gtCount, "GT: Should find 2 documents with pincode > 500000");

      Query ltQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.NUMBER, "seller", "address", "pincode"),
                      LT,
                      ConstantExpression.of(500000)))
              .build();

      long ltCount = flatCollection.count(ltQuery);
      // SELECT COUNT(*) FROM (SELECT * FROM "myTestFlat" WHERE CAST
      // ("props"->'seller'->'address'->>'pincode' AS NUMERIC) < ('500000'::int4))
      // p(countWithParser)
      assertEquals(2, ltCount, "LT: Should find 2 documents with pincode < 500000");

      Query gteQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.NUMBER, "seller", "address", "pincode"),
                      GTE,
                      ConstantExpression.of(700000)))
              .build();

      long gteCount = flatCollection.count(gteQuery);
      // SELECT COUNT(*) FROM (SELECT * FROM "myTestFlat" WHERE CAST
      // ("props"->'seller'->'address'->>'pincode' AS NUMERIC) >= ('700000'::int4))
      // p(countWithParser)
      assertEquals(2, gteCount, "GTE: Should find 2 documents with pincode >= 700000");

      Query lteQuery =
          Query.builder()
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.NUMBER, "seller", "address", "pincode"),
                      LTE,
                      ConstantExpression.of(400004)))
              .build();

      long lteCount = flatCollection.count(lteQuery);
      // SELECT COUNT(*) FROM (SELECT * FROM "myTestFlat" WHERE CAST
      // ("props"->'seller'->'address'->>'pincode' AS NUMERIC) <= ('400004'::int4))
      // p(countWithParser)
      assertEquals(2, lteCount, "LTE: Should find 2 documents with pincode <= 400004");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testUnnest(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test UNNEST on JSONB array field: props.colors
      // Expected: Should unnest colors and count distinct items with colors
      // Data: id=1 has ["Blue", "Green"], id=3 has ["Black"], id=5 has ["Orange", "Blue"]
      // Total: 5 color entries from 3 items
      Query unnestJsonbQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(JsonIdentifierExpression.of("props", "colors"))
              .addFromClause(
                  UnnestExpression.of(JsonIdentifierExpression.of("props", "colors"), false))
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(unnestJsonbQuery);

      long count = 0;
      while (resultIterator.hasNext()) {
        resultIterator.next();
        count++;
      }

      // Expecting 5 results: 2 from Soap (Blue, Green), 1 from Shampoo (Black),
      // 2 from Lifebuoy (Orange, Blue)
      assertEquals(5, count, "Should find 5 color entries after unnesting JSONB arrays");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testGroupByScalarField(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test GROUP BY on JSONB scalar field: props.brand
      // This tests grouping by a nested string field in a JSONB column
      // Data: 3 rows have brands (Dettol, Sunsilk, Lifebuoy), 7 rows have NULL/missing brand
      // GROUP BY on JSONB fields groups NULL values together (standard SQL behavior)
      Query groupByBrandQuery =
          Query.builder()
              .addSelection(JsonIdentifierExpression.of("props", "brand"))
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "count")
              .addAggregation(JsonIdentifierExpression.of("props", "brand"))
              .addSort(JsonIdentifierExpression.of("props", "brand"), ASC)
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(groupByBrandQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_jsonb_group_by_brand_test_response.json", 4);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testGroupByArray(String dataStoreName) throws IOException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test GROUP BY on JSONB array field: props.colors with UNNEST
      // This tests grouping by individual elements (after unnesting) in a JSONB array
      // Behavior should match nested collections: UNNEST flattens array, GROUP BY groups elements
      // Data: Row 1 has ["Blue", "Green"], Row 3 has ["Black"], Row 5 has ["Orange", "Blue"]
      // Expected: Blue (2), Green (1), Black (1), Orange (1) - 4 distinct color groups
      Query groupByColorsQuery =
          Query.builder()
              .addSelection(JsonIdentifierExpression.of("props", "colors"), "color")
              .addSelection(AggregateExpression.of(COUNT, ConstantExpression.of(1)), "count")
              .addFromClause(
                  UnnestExpression.of(JsonIdentifierExpression.of("props", "colors"), false))
              .addAggregation(JsonIdentifierExpression.of("props", "colors"))
              .addSort(JsonIdentifierExpression.of("props", "colors"), ASC)
              .build();

      Iterator<Document> resultIterator = flatCollection.aggregate(groupByColorsQuery);
      assertDocsAndSizeEqualWithoutOrder(
          dataStoreName, resultIterator, "query/flat_jsonb_group_by_colors_test_response.json", 4);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testAnyOnArray(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Test ArrayRelationalFilterExpression.ANY on JSONB array (props.colors)
      // This uses jsonb_array_elements() internally
      Query jsonbArrayQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .setFilter(
                  ArrayRelationalFilterExpression.builder()
                      .operator(ArrayOperator.ANY)
                      .filter(
                          RelationalExpression.of(
                              JsonIdentifierExpression.of("props", "colors"),
                              EQ,
                              ConstantExpression.of("Blue")))
                      .build())
              .build();

      long count = flatCollection.count(jsonbArrayQuery);
      // ids 1 and 5 have "Blue" in their colors array
      assertEquals(2, count, "Should find 2 items with 'Blue' color (ids 1, 5)");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testInOnUnnestedArray(String dataStoreName) throws Exception {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query unnestQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(
                  JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "source-loc"))
              .addFromClause(
                  UnnestExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                      true))
              // Should return unnested source-loc elements that match 'warehouse-A' OR 'store-1'
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                      IN,
                      ConstantExpression.ofStrings(List.of("warehouse-A", "store-1"))))
              .build();

      Iterator<Document> resultIterator = flatCollection.find(unnestQuery);

      int count = 0;
      while (resultIterator.hasNext()) {
        Document doc = resultIterator.next();
        assertNotNull(doc);
        // Parse JSON to extract the unnested value
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        // The unnested value is aliased as "props.source-loc"
        JsonNode locationNode = json.get("props.source-loc");
        count++;
      }

      assertEquals(2, count, "Should return at least 2 unnested locations matching the filter");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotInOnUnnestedArray(String dataStoreName) throws Exception {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query unnestQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(
                  JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "source-loc"))
              .addFromClause(
                  UnnestExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                      true))
              // Should return unnested source-loc elements that DO NOT match 'warehouse-A'
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                      NOT_IN,
                      ConstantExpression.ofStrings(List.of("warehouse-A"))))
              .build();

      Iterator<Document> resultIterator = flatCollection.find(unnestQuery);

      int count = 0;
      while (resultIterator.hasNext()) {
        Document doc = resultIterator.next();
        assertNotNull(doc);
        // Parse JSON to extract the unnested value
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        JsonNode locationNode = json.get("props.source-loc");
        count++;
      }
      // Should NOT contain 'warehouse-A'
      assertEquals(12, count, "Should return unnested locations not matching the filter");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testExistsOnArrays(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Query using EXISTS on JSONB array field
      // props.colors has: non-empty (rows 1, 3, 5), empty (row 7), NULL (rest)
      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(JsonIdentifierExpression.of("props", "colors"))
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors"),
                      EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        // Verify that ALL returned documents have non-empty arrays in props.colors
        JsonNode props = json.get("props");
        assertTrue(props.isObject(), "props should be a JSON object");

        JsonNode colors = props.get("colors");
        assertTrue(
            colors.isArray() && !colors.isEmpty(),
            "colors should be non-empty array, but was: " + colors);
      }

      // Should return rows 1, 2, 3 which have non-empty colors arrays
      assertEquals(3, count, "Should return exactly 3 documents with non-empty colors");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotExistsOnArrays(String dataStoreName) throws JsonProcessingException {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      // Query using NOT_EXISTS on JSONB array field
      // Test with props.colors field
      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(JsonIdentifierExpression.of("props", "colors"))
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING_ARRAY, "colors"),
                      NOT_EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      Set<String> returnedItems = new HashSet<>();
      while (results.hasNext()) {
        Document doc = results.next();
        JsonNode json = new ObjectMapper().readTree(doc.toJson());
        count++;

        String item = json.get("item").asText();
        returnedItems.add(item);

        // Verify that returned documents have NULL parent, missing field, or empty arrays
        JsonNode props = json.get("props");
        if (props != null && props.isObject()) {
          JsonNode colors = props.get("colors");
          assertTrue(
              colors == null || !colors.isArray() || colors.isEmpty(),
              "colors should be NULL or empty array for item: " + item + ", but was: " + colors);
        }
        // NULL props is also valid (if props is null, then props->colours is null too)
      }

      // Should include documents where props is NULL or props.colors is NULL/empty
      assertTrue(count > 0, "Should return at least some documents");
      assertTrue(
          returnedItems.contains("Comb"), "Should include Comb (has empty colors array in props)");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testExistsOnScalars(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(
                  JsonIdentifierExpression.of("props", JsonFieldType.STRING, "product-code"))
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING, "product-code"),
                      EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        Document next = results.next();
        count++;
      }
      // We have 4 rows with "props"->'product-code' field present (regardless of the value)
      assertEquals(4, count, "Should return exactly 4 documents with non-empty product-code");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotExistsOnScalars(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query query =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addSelection(
                  JsonIdentifierExpression.of("props", JsonFieldType.STRING, "product-code"))
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of("props", JsonFieldType.STRING, "product-code"),
                      NOT_EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> results = flatCollection.find(query);

      int count = 0;
      while (results.hasNext()) {
        Document next = results.next();
        count++;
      }
      // We have 6 rows that have "props"->'product-code' field missing
      assertEquals(6, count, "Should return exactly 6 documents with missing product-code");
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testExistsOnUnnestedArray(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query unnestQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addFromClause(
                  UnnestExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                      true))
              // Should include only those props->source_loc arrays that have at least one element.
              // So essentially, after unnesting this array, we don't have any rows with NULL for
              // the unnested col
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                      EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> resultIterator = flatCollection.find(unnestQuery);

      int count = 0;
      while (resultIterator.hasNext()) {
        Document doc = resultIterator.next();
        assertNotNull(doc);
        count++;
      }
      assertEquals(6, count);
    }

    @ParameterizedTest
    @ArgumentsSource(PostgresProvider.class)
    void testNotExistsOnUnnestedArray(String dataStoreName) {
      Datastore datastore = datastoreMap.get(dataStoreName);
      Collection flatCollection =
          datastore.getCollectionForType(FLAT_COLLECTION_NAME, DocumentType.FLAT);

      Query unnestQuery =
          Query.builder()
              .addSelection(IdentifierExpression.of("item"))
              .addFromClause(
                  UnnestExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                      true))
              // Should include only those props->source_loc arrays that are either NULL or empty.
              .setFilter(
                  RelationalExpression.of(
                      JsonIdentifierExpression.of(
                          "props", JsonFieldType.STRING_ARRAY, "source-loc"),
                      NOT_EXISTS,
                      ConstantExpression.of("null")))
              .build();

      Iterator<Document> resultIterator = flatCollection.find(unnestQuery);

      int count = 0;
      while (resultIterator.hasNext()) {
        Document doc = resultIterator.next();
        assertNotNull(doc);
        count++;
      }
      assertEquals(7, count);
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

  @ParameterizedTest
  @ArgumentsSource(MongoProvider.class)
  public void testMongoFunctionExpressionGroupBy(String dataStoreName) throws Exception {
    Collection collection = getCollection(dataStoreName);

    FunctionExpression functionExpression =
        FunctionExpression.builder()
            .operator(FLOOR)
            .operand(
                FunctionExpression.builder()
                    .operator(DIVIDE)
                    .operand(
                        FunctionExpression.builder()
                            .operator(SUBTRACT)
                            .operand(IdentifierExpression.of("price"))
                            .operand(ConstantExpression.of(5))
                            .build())
                    .operand(ConstantExpression.of(5))
                    .build())
            .build();
    List<SelectionSpec> selectionSpecs =
        List.of(
            SelectionSpec.of(functionExpression, "function"),
            SelectionSpec.of(
                AggregateExpression.of(COUNT, IdentifierExpression.of("function")),
                "functionCount"));
    Selection selection = Selection.builder().selectionSpecs(selectionSpecs).build();

    Query query =
        Query.builder()
            .setSelection(selection)
            .setAggregation(
                Aggregation.builder().expression(IdentifierExpression.of("function")).build())
            .setSort(
                Sort.builder()
                    .sortingSpec(SortingSpec.of(IdentifierExpression.of("function"), ASC))
                    .build())
            .build();

    Iterator<Document> resultDocs = collection.aggregate(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, "query/function_expression_group_by_response.json", 3);

    testCountApi(dataStoreName, query, "query/function_expression_group_by_response.json");
  }

  @ParameterizedTest
  @ArgumentsSource(MongoProvider.class)
  public void testToLowerCaseMongoFunctionOperator(String dataStoreName) throws Exception {
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
                    FunctionExpression.builder()
                        .operator(FunctionOperator.TO_LOWER_CASE)
                        .operand(IdentifierExpression.of("item"))
                        .build(),
                    RelationalOperator.EQ,
                    ConstantExpression.of("shampoo")))
            .build();

    Query query = Query.builder().setSelection(selection).setFilter(filter).build();

    Iterator<Document> resultDocs = collection.find(query);
    assertDocsAndSizeEqualWithoutOrder(
        dataStoreName, resultDocs, "query/case_insensitive_exact_match_response.json", 2);
  }

  @ParameterizedTest
  @ArgumentsSource(MongoProvider.class)
  void testSelfJoinWithSubQuery(String dataStoreName) throws IOException {
    Collection collection = getCollection(dataStoreName);

    /*
    This is the query we want to execute:
    SELECT item, quantity, date
    FROM <implicit_collection>
    JOIN (
        SELECT item, MAX(date) AS latest_date
        FROM <implicit_collection>
        GROUP BY item
    ) latest
    ON item = latest.item
    AND date = latest.latest_date
    ORDER BY `item` ASC;
    */

    /*
    The right subquery:
    SELECT item, MAX(date) AS latest_date
    FROM <implicit_collection>
    GROUP BY item
    */
    Query subQuery =
        Query.builder()
            .addSelection(SelectionSpec.of(IdentifierExpression.of("item")))
            .addSelection(
                SelectionSpec.of(
                    AggregateExpression.of(
                        AggregationOperator.MAX, IdentifierExpression.of("date")),
                    "latest_date"))
            .addAggregation(IdentifierExpression.of("item"))
            .build();

    /*
    The FROM expression representing a join with the right subquery:
    FROM <implicit_collection>
    JOIN (
       SELECT item, MAX(date) AS latest_date
       FROM <implicit_collection>
       GROUP BY item
    ) latest
    ON item = latest.item
    AND date = latest.latest_date;
    */
    SubQueryJoinExpression subQueryJoinExpression =
        SubQueryJoinExpression.builder()
            .subQuery(subQuery)
            .subQueryAlias("latest")
            .joinCondition(
                LogicalExpression.and(
                    RelationalExpression.of(
                        IdentifierExpression.of("item"),
                        RelationalOperator.EQ,
                        AliasedIdentifierExpression.builder()
                            .name("item")
                            .contextAlias("latest")
                            .build()),
                    RelationalExpression.of(
                        IdentifierExpression.of("date"),
                        RelationalOperator.EQ,
                        AliasedIdentifierExpression.builder()
                            .name("latest_date")
                            .contextAlias("latest")
                            .build())))
            .build();

    /*
    Now build the top-level Query:
    SELECT item, quantity, date FROM <subQueryJoinExpression> ORDER BY `item` ASC;
    */
    Query mainQuery =
        Query.builder()
            .addSelection(IdentifierExpression.of("item"))
            .addSelection(IdentifierExpression.of("quantity"))
            .addSelection(IdentifierExpression.of("date"))
            .addFromClause(subQueryJoinExpression)
            .addSort(IdentifierExpression.of("item"), ASC)
            .build();

    Iterator<Document> iterator = collection.aggregate(mainQuery);
    assertDocsAndSizeEqual(
        dataStoreName, iterator, "query/self_join_with_sub_query_response.json", 4);
  }

  @ParameterizedTest
  @ArgumentsSource(MongoProvider.class)
  void testSelfJoinWithSubQueryWithNestedFields(String dataStoreName) throws IOException {
    createCollectionData(
        "query/items_data_with_nested_fields.json", "items_data_with_nested_fields");
    Collection collection = getCollection(dataStoreName, "items_data_with_nested_fields");

    /*
    This is the query we want to execute:
    SELECT itemDetails.item, itemDetails.quantity, itemDetails.date
    FROM <implicit_collection>
    JOIN (
        SELECT itemDetails.item, MAX(itemDetails.date) AS latest_date
        FROM <implicit_collection>
        GROUP BY itemDetails.item
    ) latest
    ON itemDetails.item = latest.itemDetails.item
    AND itemDetails.date = latest.latest_date
    ORDER BY `itemDetails.item` ASC;
    */

    /*
    The right subquery:
    SELECT itemDetails.item, MAX(itemDetails.date) AS latest_date
    FROM <implicit_collection>
    GROUP BY itemDetails.item
    */
    Query subQuery =
        Query.builder()
            .addSelection(SelectionSpec.of(IdentifierExpression.of("itemDetails.item")))
            .addSelection(
                SelectionSpec.of(
                    AggregateExpression.of(
                        AggregationOperator.MAX, IdentifierExpression.of("itemDetails.date")),
                    "latest_date"))
            .addAggregation(IdentifierExpression.of("itemDetails.item"))
            .build();

    /*
    The FROM expression representing a join with the right subquery:
    FROM <implicit_collection>
    JOIN (
       SELECT itemDetails.item, MAX(itemDetails.date) AS latest_date
       FROM <implicit_collection>
       GROUP BY itemDetails.item
    ) latest
    ON itemDetails.item = latest.itemDetails.item
    AND itemDetails.date = latest.latest_date;
    */
    SubQueryJoinExpression subQueryJoinExpression =
        SubQueryJoinExpression.builder()
            .subQuery(subQuery)
            .subQueryAlias("latest")
            .joinCondition(
                LogicalExpression.and(
                    RelationalExpression.of(
                        IdentifierExpression.of("itemDetails.item"),
                        RelationalOperator.EQ,
                        AliasedIdentifierExpression.builder()
                            .name("itemDetails.item")
                            .contextAlias("latest")
                            .build()),
                    RelationalExpression.of(
                        IdentifierExpression.of("itemDetails.date"),
                        RelationalOperator.EQ,
                        AliasedIdentifierExpression.builder()
                            .name("latest_date")
                            .contextAlias("latest")
                            .build())))
            .build();

    /*
    Now build the top-level Query:
    SELECT itemDetails.item, itemDetails.quantity, itemDetails.date FROM <subQueryJoinExpression> ORDER BY `itemDetails.item` ASC;
    */
    Query mainQuery =
        Query.builder()
            .addSelection(IdentifierExpression.of("itemDetails.item"))
            .addSelection(IdentifierExpression.of("itemDetails.quantity"))
            .addSelection(IdentifierExpression.of("itemDetails.date"))
            .addFromClause(subQueryJoinExpression)
            .addSort(IdentifierExpression.of("itemDetails.item"), ASC)
            .build();

    Iterator<Document> iterator = collection.aggregate(mainQuery);
    assertDocsAndSizeEqual(
        dataStoreName, iterator, "query/sub_query_join_response_with_nested_fields.json", 3);
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
